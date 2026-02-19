"""
PDF Cleanup Lambda Function

Triggered when a PDF is deleted from s3://bucket/pdf/[folder]/filename.pdf
Automatically deletes the corresponding temp folder at s3://bucket/temp/[folder]/[filename minus extension]/

Features:
- Deletes all temp files associated with the deleted PDF
- Logs all actions to CloudWatch
- Identifies who deleted the PDF via CloudTrail
- Sends email notification to the user (if configured)
"""

import json
import os
import boto3
import logging
from datetime import datetime, timedelta
from typing import Optional
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
cloudtrail = boto3.client('cloudtrail')
ses = boto3.client('ses')
logs = boto3.client('logs')

# Environment variables
NOTIFICATION_TABLE = os.environ.get('NOTIFICATION_TABLE', 'pdf-cleanup-notifications')
LOG_GROUP_NAME = os.environ.get('LOG_GROUP_NAME', '/pdf-processing/cleanup')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'pdf-cleanup@example.com')


def get_temp_folder_path(pdf_key: str) -> Optional[str]:
    """
    Convert a PDF path to its corresponding temp folder path.
    
    Example:
        pdf/reports-2025/quarterly-report.pdf -> temp/reports-2025/quarterly-report/
    """
    if not pdf_key.startswith('pdf/'):
        logger.warning(f"Unexpected PDF path format: {pdf_key}")
        return None
    
    # Remove 'pdf/' prefix and '.pdf' extension
    relative_path = pdf_key[4:]  # Remove 'pdf/'
    
    if relative_path.lower().endswith('.pdf'):
        relative_path = relative_path[:-4]  # Remove '.pdf'
    
    return f"temp/{relative_path}/"


def delete_temp_folder(bucket: str, temp_prefix: str) -> int:
    """
    Delete all objects under the temp folder prefix.
    Returns the number of objects deleted.
    """
    deleted_count = 0
    
    try:
        # List all objects with the prefix
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=temp_prefix):
            if 'Contents' not in page:
                continue
            
            # Delete objects in batches of 1000 (S3 limit)
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            
            if objects_to_delete:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects_to_delete}
                )
                deleted_count += len(objects_to_delete)
                logger.info(f"Deleted {len(objects_to_delete)} objects from {temp_prefix}")
        
        logger.info(f"Total objects deleted from {temp_prefix}: {deleted_count}")
        
    except ClientError as e:
        logger.error(f"Error deleting temp folder {temp_prefix}: {e}")
        raise
    
    return deleted_count


def get_deletion_user(bucket: str, key: str, event_time: datetime) -> dict:
    """
    Query CloudTrail to find who deleted the S3 object.
    
    Returns dict with 'username', 'arn', and 'type'.
    """
    try:
        response = cloudtrail.lookup_events(
            LookupAttributes=[
                {'AttributeKey': 'EventName', 'AttributeValue': 'DeleteObject'},
            ],
            StartTime=event_time - timedelta(minutes=15),
            EndTime=event_time + timedelta(minutes=5),
            MaxResults=50
        )
        
        # Find the matching event for our bucket/key
        for event in response.get('Events', []):
            cloud_trail_event = json.loads(event['CloudTrailEvent'])
            
            # Check if this event matches our bucket and key
            request_params = cloud_trail_event.get('requestParameters', {})
            if (request_params.get('bucketName') == bucket and 
                request_params.get('key') == key):
                
                user_identity = cloud_trail_event.get('userIdentity', {})
                arn = user_identity.get('arn', '')
                
                # Extract username from ARN
                # ARN format: arn:aws:iam::123456789:user/jane.doe
                username = 'unknown'
                if '/' in arn:
                    username = arn.split('/')[-1]
                elif 'userName' in user_identity:
                    username = user_identity['userName']
                
                return {
                    'username': username,
                    'arn': arn,
                    'type': user_identity.get('type', 'unknown')
                }
        
        logger.warning(f"Could not find CloudTrail event for {bucket}/{key}")
        return {'username': 'unknown', 'arn': '', 'type': 'unknown'}
        
    except ClientError as e:
        logger.error(f"Error querying CloudTrail: {e}")
        return {'username': 'unknown', 'arn': '', 'type': 'unknown'}


def get_user_email(username: str) -> Optional[str]:
    """
    Look up the user's email from DynamoDB.
    Returns None if user not found or notifications disabled.
    """
    try:
        table = dynamodb.Table(NOTIFICATION_TABLE)
        response = table.get_item(Key={'iam_username': username})
        
        if 'Item' not in response:
            logger.info(f"No notification config found for user: {username}")
            return None
        
        item = response['Item']
        
        if not item.get('enabled', False):
            logger.info(f"Notifications disabled for user: {username}")
            return None
        
        return item.get('email')
        
    except ClientError as e:
        logger.error(f"Error looking up user email: {e}")
        return None


def send_notification_email(
    recipient: str,
    pdf_key: str,
    temp_folder: str,
    files_deleted: int,
    username: str,
    deletion_time: str
) -> bool:
    """
    Send email notification about the cleanup.
    """
    filename = pdf_key.split('/')[-1]
    
    subject = f"PDF Cleanup Complete: {filename}"
    
    body_text = f"""PDF Deletion Summary
====================

The following PDF and its associated temporary files have been deleted:

PDF File: {pdf_key}
Deleted At: {deletion_time}
Deleted By: {username}

Temporary Files Cleaned Up:
- Folder: {temp_folder}
- Files Deleted: {files_deleted}

This is an automated notification. No action is required.
"""

    body_html = f"""
<html>
<head></head>
<body>
    <h2>PDF Deletion Summary</h2>
    <p>The following PDF and its associated temporary files have been deleted:</p>
    
    <table style="border-collapse: collapse; margin: 20px 0;">
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; background: #f5f5f5;"><strong>PDF File</strong></td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>{pdf_key}</code></td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; background: #f5f5f5;"><strong>Deleted At</strong></td>
            <td style="padding: 8px; border: 1px solid #ddd;">{deletion_time}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; background: #f5f5f5;"><strong>Deleted By</strong></td>
            <td style="padding: 8px; border: 1px solid #ddd;">{username}</td>
        </tr>
    </table>
    
    <h3>Temporary Files Cleaned Up</h3>
    <ul>
        <li><strong>Folder:</strong> <code>{temp_folder}</code></li>
        <li><strong>Files Deleted:</strong> {files_deleted}</li>
    </ul>
    
    <p style="color: #666; font-size: 12px; margin-top: 30px;">
        This is an automated notification. No action is required.
    </p>
</body>
</html>
"""

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
        logger.info(f"Notification email sent to {recipient}")
        return True
        
    except ClientError as e:
        logger.error(f"Error sending email to {recipient}: {e}")
        return False


def log_cleanup_event(
    pdf_key: str,
    temp_folder: str,
    files_deleted: int,
    user_info: dict,
    email_sent: bool,
    email_recipient: Optional[str]
):
    """
    Log the cleanup event to CloudWatch in structured JSON format.
    """
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'event_type': 'PDF_DELETED',
        'deleted_pdf': pdf_key,
        'deleted_temp_folder': temp_folder,
        'temp_files_deleted': files_deleted,
        'deleted_by': user_info['username'],
        'deleted_by_arn': user_info['arn'],
        'deletion_method': user_info['type'],
        'email_sent': email_sent,
        'email_recipient': email_recipient or 'N/A'
    }
    
    try:
        # Ensure log stream exists
        log_stream_name = datetime.utcnow().strftime('%Y/%m/%d')
        
        try:
            logs.create_log_stream(
                logGroupName=LOG_GROUP_NAME,
                logStreamName=log_stream_name
            )
        except logs.exceptions.ResourceAlreadyExistsException:
            pass  # Stream already exists
        
        # Put log event
        logs.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=log_stream_name,
            logEvents=[{
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'message': json.dumps(log_entry)
            }]
        )
        
        logger.info(f"Logged cleanup event: {json.dumps(log_entry)}")
        
    except ClientError as e:
        logger.error(f"Error logging to CloudWatch: {e}")


def handler(event, context):
    """
    Lambda handler for S3 delete events.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    for record in event.get('Records', []):
        # Extract S3 event details
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        event_time = datetime.fromisoformat(
            record['eventTime'].replace('Z', '+00:00')
        ).replace(tzinfo=None)
        
        logger.info(f"Processing deletion of {bucket}/{key}")
        
        # Skip if not a PDF in the pdf/ folder
        if not key.startswith('pdf/') or not key.lower().endswith('.pdf'):
            logger.info(f"Skipping non-PDF file: {key}")
            continue
        
        # Get the corresponding temp folder path
        temp_folder = get_temp_folder_path(key)
        if not temp_folder:
            logger.warning(f"Could not determine temp folder for: {key}")
            continue
        
        logger.info(f"Will delete temp folder: {temp_folder}")
        
        # Delete the temp folder and contents
        files_deleted = delete_temp_folder(bucket, temp_folder)
        
        # Get info about who deleted the PDF
        user_info = get_deletion_user(bucket, key, event_time)
        logger.info(f"PDF deleted by: {user_info['username']} ({user_info['arn']})")
        
        # Send email notification if configured
        email_sent = False
        email_recipient = None
        
        if user_info['username'] != 'unknown':
            email_recipient = get_user_email(user_info['username'])
            if email_recipient:
                email_sent = send_notification_email(
                    recipient=email_recipient,
                    pdf_key=key,
                    temp_folder=temp_folder,
                    files_deleted=files_deleted,
                    username=user_info['username'],
                    deletion_time=event_time.strftime('%Y-%m-%d %H:%M:%S UTC')
                )
        
        # Log the cleanup event
        log_cleanup_event(
            pdf_key=key,
            temp_folder=temp_folder,
            files_deleted=files_deleted,
            user_info=user_info,
            email_sent=email_sent,
            email_recipient=email_recipient
        )
        
        logger.info(
            f"Cleanup complete for {key}: "
            f"{files_deleted} temp files deleted, "
            f"email_sent={email_sent}"
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Cleanup complete')
    }
