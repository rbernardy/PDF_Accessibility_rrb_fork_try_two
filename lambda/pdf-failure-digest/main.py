"""
PDF Failure Digest Lambda

Triggered daily at 11:55 PM to send digest emails to users whose PDFs failed processing.
Groups all failures by user and sends one summary email per user.
"""

import json
import os
import boto3
import logging
from datetime import datetime
from collections import defaultdict
from typing import Optional
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

# Environment variables
FAILURE_TABLE = os.environ.get('FAILURE_TABLE', 'pdf-failure-records')
NOTIFICATION_TABLE = os.environ.get('NOTIFICATION_TABLE', 'pdf-cleanup-notifications')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'pdf-cleanup@example.com')


def get_todays_failures() -> list:
    """Query DynamoDB for all failures from today that haven't been notified."""
    table = dynamodb.Table(FAILURE_TABLE)
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
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
    """Look up user's email from notification preferences table."""
    try:
        table = dynamodb.Table(NOTIFICATION_TABLE)
        response = table.get_item(Key={'iam_username': username})
        
        if 'Item' not in response:
            logger.info(f"No notification config for user: {username}")
            return None
        
        item = response['Item']
        if not item.get('enabled', False):
            logger.info(f"Notifications disabled for user: {username}")
            return None
        
        return item.get('email')
        
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


def format_failure_entry(failure: dict, index: int) -> str:
    """Format a single failure entry for the email."""
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


def send_digest_email(recipient: str, username: str, failures: list, date: str) -> bool:
    """Send digest email to user with all their failures."""
    
    # Build text version
    failure_entries_text = ""
    for i, failure in enumerate(failures, 1):
        failure_entries_text += format_failure_entry(failure, i)
    
    body_text = f"""PDF Processing Failure Summary
==============================

Date: {date}
User: {username}

The following PDFs failed processing and have been automatically cleaned up:
{failure_entries_text}

Total failures today: {len(failures)}

To retry processing, please re-upload the original PDF files to the appropriate folder.

This is an automated notification.
"""

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
            Source=SENDER_EMAIL,
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
    Lambda handler for daily digest emails.
    Triggered by EventBridge schedule at 11:55 PM.
    """
    logger.info("Starting daily failure digest processing")
    
    # Get today's date for the email
    today = datetime.utcnow().strftime('%B %d, %Y')
    
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
    emails_sent = 0
    failure_ids_notified = []
    
    for username, user_failures in failures_by_user.items():
        # Get user's email
        email = get_user_email(username)
        
        if not email:
            logger.warning(f"No email configured for user {username}, skipping {len(user_failures)} failures")
            # Still mark as notified to avoid re-processing
            for f in user_failures:
                failure_ids_notified.append(f['failure_id'])
            continue
        
        # Send digest email
        if send_digest_email(email, username, user_failures, today):
            emails_sent += 1
            for f in user_failures:
                failure_ids_notified.append(f['failure_id'])
    
    # Mark all processed failures as notified
    if failure_ids_notified:
        mark_as_notified(failure_ids_notified)
    
    logger.info(f"Digest complete: sent {emails_sent} emails, processed {len(failure_ids_notified)} failures")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'emails_sent': emails_sent,
            'failures_processed': len(failure_ids_notified),
            'users_processed': len(failures_by_user)
        })
    }
