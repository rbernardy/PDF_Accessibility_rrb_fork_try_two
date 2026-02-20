"""
PDF Failure Digest Lambda

Triggered daily at 11:55 PM to send digest reports to users whose PDFs failed processing.
Groups all failures by user and either:
- Sends email (if email feature is enabled)
- Saves report to S3 (if email feature is disabled)

Configuration via SSM Parameter Store:
- /pdf-processing/email-enabled: "true" or "false"
- /pdf-processing/sender-email: sender email address (if email enabled)
"""

import json
import os
import boto3
import logging
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
ssm = boto3.client('ssm')
s3 = boto3.client('s3')

# Environment variables
FAILURE_TABLE = os.environ.get('FAILURE_TABLE', 'pdf-failure-records')
NOTIFICATION_TABLE = os.environ.get('NOTIFICATION_TABLE', 'pdf-cleanup-notifications')
SENDER_EMAIL_PARAM = os.environ.get('SENDER_EMAIL_PARAM', '/pdf-processing/sender-email')
EMAIL_ENABLED_PARAM = os.environ.get('EMAIL_ENABLED_PARAM', '/pdf-processing/email-enabled')
BUCKET_NAME = os.environ.get('BUCKET_NAME', '')

# Cache for SSM parameters (avoid repeated calls within same invocation)
_ssm_cache = {}


def get_ssm_parameter(param_name: str, default: str = None) -> Optional[str]:
    """Get parameter from SSM Parameter Store (cached)."""
    if param_name in _ssm_cache:
        return _ssm_cache[param_name]
    
    try:
        response = ssm.get_parameter(Name=param_name)
        value = response['Parameter']['Value']
        _ssm_cache[param_name] = value
        logger.info(f"Loaded SSM parameter {param_name}: {value}")
        return value
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            logger.warning(f"SSM parameter {param_name} not found, using default: {default}")
            return default
        logger.error(f"Error getting SSM parameter {param_name}: {e}")
        return default


def is_email_enabled() -> bool:
    """Check if email feature is enabled via SSM."""
    value = get_ssm_parameter(EMAIL_ENABLED_PARAM, 'false')
    return value.lower() == 'true'


def get_sender_email() -> str:
    """Get sender email from SSM Parameter Store."""
    return get_ssm_parameter(SENDER_EMAIL_PARAM, 'sender-email-not-configured@example.com')


def get_todays_failures() -> list:
    """Query DynamoDB for all failures from today that haven't been notified."""
    table = dynamodb.Table(FAILURE_TABLE)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    try:
        response = table.query(
            IndexName='failure_date-index',
            KeyConditionExpression='failure_date = :date',
            FilterExpression='notified = :notified',
            ExpressionAttributeValues={
                ':date': today,
                ':notified': False
            }
        )
        
        failures = response.get('Items', [])
        logger.info(f"Found {len(failures)} unnotified failures for {today}")
        return failures
        
    except ClientError as e:
        logger.error(f"Error querying failures: {e}")
        return []


def get_user_email(username: str) -> Optional[str]:
    """
    Look up user's email from notification preferences table.
    Falls back to 'default' user if specific user not found.
    """
    try:
        table = dynamodb.Table(NOTIFICATION_TABLE)
        
        # First try the specific user
        if username and username != 'unknown':
            response = table.get_item(Key={'iam_username': username})
            
            if 'Item' in response:
                item = response['Item']
                if item.get('enabled', False):
                    return item.get('email')
                else:
                    logger.info(f"Notifications disabled for user: {username}")
        
        # Fall back to 'default' user (receives all unmatched notifications)
        response = table.get_item(Key={'iam_username': 'default'})
        
        if 'Item' in response:
            item = response['Item']
            if item.get('enabled', False):
                logger.info(f"Using default recipient for user: {username}")
                return item.get('email')
        
        logger.info(f"No notification config for user: {username} (and no default)")
        return None
        
    except ClientError as e:
        logger.error(f"Error looking up user email: {e}")
        return None


def mark_as_notified(failure_ids: list):
    """Mark failure records as notified."""
    table = dynamodb.Table(FAILURE_TABLE)
    
    for failure_id in failure_ids:
        try:
            table.update_item(
                Key={'failure_id': failure_id},
                UpdateExpression='SET notified = :notified',
                ExpressionAttributeValues={':notified': True}
            )
        except ClientError as e:
            logger.error(f"Error marking {failure_id} as notified: {e}")


def strip_srv_prefix(username: str) -> str:
    """Remove 'srv-' prefix from username if present."""
    if username and username.lower().startswith('srv-'):
        return username[4:]
    return username or 'unknown'


def format_failure_entry(failure: dict, index: int) -> str:
    """Format a single failure entry for the report."""
    pdf_key = failure.get('pdf_key', 'unknown')
    filename = pdf_key.split('/')[-1] if pdf_key else 'unknown'
    
    return f"""
{index}. {filename}
   - Original location: {pdf_key}
   - Failure reason: {failure.get('failure_reason', 'Unknown')}
   - Temp files deleted: {failure.get('temp_files_deleted', 0)}
   - Failed at: {failure.get('timestamp', 'Unknown')}
"""


def format_failure_entry_html(failure: dict, index: int) -> str:
    """Format a single failure entry for HTML email."""
    pdf_key = failure.get('pdf_key', 'unknown')
    filename = pdf_key.split('/')[-1] if pdf_key else 'unknown'
    
    return f"""
    <tr>
        <td style="padding: 10px; border-bottom: 1px solid #eee;">
            <strong>{index}. {filename}</strong><br>
            <span style="color: #666; font-size: 12px;">
                Location: <code>{pdf_key}</code><br>
                Reason: {failure.get('failure_reason', 'Unknown')}<br>
                Temp files deleted: {failure.get('temp_files_deleted', 0)}<br>
                Failed at: {failure.get('timestamp', 'Unknown')}
            </span>
        </td>
    </tr>
"""


def generate_report_text(username: str, failures: list, date: str) -> str:
    """Generate plain text report content."""
    failure_entries = ""
    for i, failure in enumerate(failures, 1):
        failure_entries += format_failure_entry(failure, i)
    
    return f"""PDF Processing Failure Summary
==============================

Date: {date}
User: {username}

The following PDFs failed processing and have been automatically cleaned up:
{failure_entries}

Total failures today: {len(failures)}

To retry processing, please re-upload the original PDF files to the appropriate folder.

This is an automated report.
"""


def save_report_to_s3(username: str, failures: list, date: str) -> bool:
    """Save failure report to S3 as a text file."""
    # Strip 'srv-' prefix from username
    clean_username = strip_srv_prefix(username)
    
    # Generate timestamp for filename: yyyyMMdd-HHmm
    now = datetime.now(timezone.utc)
    timestamp = now.strftime('%Y%m%d-%H%M')
    
    # Build filename: username-yyyyMMdd-HHmm.txt
    filename = f"{clean_username}-{timestamp}.txt"
    
    # Build S3 key: reports/deletion_reports/username/filename
    s3_key = f"reports/deletion_reports/{clean_username}/{filename}"
    
    # Generate report content
    report_content = generate_report_text(username, failures, date)
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=report_content.encode('utf-8'),
            ContentType='text/plain'
        )
        logger.info(f"Saved report to s3://{BUCKET_NAME}/{s3_key}")
        return True
        
    except ClientError as e:
        logger.error(f"Error saving report to S3: {e}")
        return False


def send_digest_email(recipient: str, username: str, failures: list, date: str) -> bool:
    """Send digest email to user with all their failures."""
    
    # Build text version
    body_text = generate_report_text(username, failures, date)

    # Build HTML version
    failure_entries_html = ""
    for i, failure in enumerate(failures, 1):
        failure_entries_html += format_failure_entry_html(failure, i)
    
    body_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
</head>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
    <h2 style="color: #d32f2f;">PDF Processing Failure Summary</h2>
    
    <table style="width: 100%; margin-bottom: 20px;">
        <tr>
            <td style="padding: 8px; background: #f5f5f5;"><strong>Date:</strong></td>
            <td style="padding: 8px;">{date}</td>
        </tr>
        <tr>
            <td style="padding: 8px; background: #f5f5f5;"><strong>User:</strong></td>
            <td style="padding: 8px;">{username}</td>
        </tr>
        <tr>
            <td style="padding: 8px; background: #f5f5f5;"><strong>Total Failures:</strong></td>
            <td style="padding: 8px;">{len(failures)}</td>
        </tr>
    </table>
    
    <p>The following PDFs failed processing and have been automatically cleaned up:</p>
    
    <table style="width: 100%; border-collapse: collapse;">
        {failure_entries_html}
    </table>
    
    <p style="margin-top: 20px; padding: 15px; background: #fff3e0; border-left: 4px solid #ff9800;">
        <strong>To retry processing:</strong> Please re-upload the original PDF files to the appropriate folder.
    </p>
    
    <p style="color: #666; font-size: 12px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 15px;">
        This is an automated notification from the PDF Accessibility Processing Pipeline.
    </p>
</body>
</html>
"""

    subject = f"PDF Processing Failures - Daily Summary for {date}"
    
    try:
        ses.send_email(
            Source=get_sender_email(),
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Text': {'Data': body_text, 'Charset': 'UTF-8'},
                    'Html': {'Data': body_html, 'Charset': 'UTF-8'}
                }
            }
        )
        logger.info(f"Sent digest email to {recipient} with {len(failures)} failures")
        return True
        
    except ClientError as e:
        logger.error(f"Error sending email to {recipient}: {e}")
        return False


def handler(event, context):
    """
    Lambda handler for daily digest.
    Triggered by EventBridge schedule at 11:55 PM.
    
    If email is enabled (SSM: /pdf-processing/email-enabled = "true"):
        - Sends email to users
    If email is disabled:
        - Saves report to S3: reports/deletion_reports/{username}/{username}-{timestamp}.txt
    """
    logger.info("Starting daily failure digest processing")
    
    # Check if email is enabled
    email_enabled = is_email_enabled()
    logger.info(f"Email feature enabled: {email_enabled}")
    
    # Get today's date for the report
    today = datetime.now(timezone.utc).strftime('%B %d, %Y')
    
    # Get all unnotified failures from today
    failures = get_todays_failures()
    
    if not failures:
        logger.info("No failures to process today")
        return {'statusCode': 200, 'body': 'No failures to process'}
    
    # Group failures by username
    failures_by_user = defaultdict(list)
    for failure in failures:
        username = failure.get('iam_username', 'unknown')
        failures_by_user[username].append(failure)
    
    logger.info(f"Processing failures for {len(failures_by_user)} users")
    
    # Process each user
    reports_generated = 0
    emails_sent = 0
    failure_ids_notified = []
    
    for username, user_failures in failures_by_user.items():
        success = False
        
        if email_enabled:
            # Try to send email
            email = get_user_email(username)
            if email:
                success = send_digest_email(email, username, user_failures, today)
                if success:
                    emails_sent += 1
            else:
                logger.warning(f"No email configured for user {username}, falling back to S3 report")
                # Fall back to S3 if no email configured
                success = save_report_to_s3(username, user_failures, today)
                if success:
                    reports_generated += 1
        else:
            # Save to S3
            success = save_report_to_s3(username, user_failures, today)
            if success:
                reports_generated += 1
        
        # Mark as notified regardless of delivery method
        if success:
            for f in user_failures:
                failure_ids_notified.append(f['failure_id'])
    
    # Mark all processed failures as notified
    if failure_ids_notified:
        mark_as_notified(failure_ids_notified)
    
    logger.info(f"Digest complete: {emails_sent} emails sent, {reports_generated} S3 reports, {len(failure_ids_notified)} failures processed")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'emails_sent': emails_sent,
            'reports_generated': reports_generated,
            'failures_processed': len(failure_ids_notified),
            'users_processed': len(failures_by_user)
        })
    }
