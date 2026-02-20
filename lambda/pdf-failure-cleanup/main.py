"""
PDF Failure Cleanup Lambda

Triggered by EventBridge when a Step Function execution fails, times out, or is aborted.
Automatically deletes the original PDF and its temp folder, then stores a failure record
for the daily digest email.
"""

import json
import os
import boto3
import logging
import uuid
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
logs = boto3.client('logs')

# Environment variables
FAILURE_TABLE = os.environ.get('FAILURE_TABLE', 'pdf-failure-records')
LOG_GROUP_NAME = os.environ.get('LOG_GROUP_NAME', '/pdf-processing/cleanup')
BUCKET_NAME = os.environ.get('BUCKET_NAME', '')


def extract_pdf_key_from_execution(execution_input: dict) -> Optional[str]:
    """
    Extract the original PDF S3 key from the Step Function execution input.
    
    The execution input typically contains:
    - s3_key: the original PDF path (e.g., "pdf/reports/document.pdf")
    - s3_bucket: the bucket name
    - chunks: array of chunk information
    """
    # Try common input field names
    if 's3_key' in execution_input:
        return execution_input['s3_key']
    if 'pdf_key' in execution_input:
        return execution_input['pdf_key']
    if 'key' in execution_input:
        return execution_input['key']
    
    # Try to extract from chunks if present
    if 'chunks' in execution_input and len(execution_input['chunks']) > 0:
        chunk = execution_input['chunks'][0]
        if 'chunk_key' in chunk:
            # Derive original PDF path from chunk path
            # chunk_key format: temp/[folder]/[filename]/chunks/chunk_001.pdf
            chunk_key = chunk['chunk_key']
            parts = chunk_key.split('/')
            if len(parts) >= 3 and parts[0] == 'temp':
                folder = parts[1]
                filename = parts[2]
                return f"pdf/{folder}/{filename}.pdf"
    
    logger.warning(f"Could not extract PDF key from execution input: {execution_input}")
    return None


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


def delete_s3_object(bucket: str, key: str) -> bool:
    """Delete a single S3 object."""
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"Error deleting s3://{bucket}/{key}: {e}")
        return False


def delete_temp_folder(bucket: str, temp_prefix: str) -> int:
    """
    Delete all objects under the temp folder prefix.
    Returns the number of objects deleted.
    """
    deleted_count = 0
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=temp_prefix):
            if 'Contents' not in page:
                continue
            
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
    
    return deleted_count


def get_uploader_info(bucket: str, key: str) -> dict:
    """
    Query CloudTrail to find who uploaded the PDF (PutObject event).
    """
    try:
        # Look back up to 90 days for the upload event
        response = cloudtrail.lookup_events(
            LookupAttributes=[
                {'AttributeKey': 'EventName', 'AttributeValue': 'PutObject'},
            ],
            StartTime=datetime.utcnow() - timedelta(days=90),
            EndTime=datetime.utcnow(),
            MaxResults=50
        )
        
        for event in response.get('Events', []):
            cloud_trail_event = json.loads(event['CloudTrailEvent'])
            request_params = cloud_trail_event.get('requestParameters', {})
            
            if (request_params.get('bucketName') == bucket and 
                request_params.get('key') == key):
                
                user_identity = cloud_trail_event.get('userIdentity', {})
                arn = user_identity.get('arn', '')
                
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
        
        logger.warning(f"Could not find CloudTrail PutObject event for {bucket}/{key}")
        return {'username': 'unknown', 'arn': '', 'type': 'unknown'}
        
    except ClientError as e:
        logger.error(f"Error querying CloudTrail: {e}")
        return {'username': 'unknown', 'arn': '', 'type': 'unknown'}


def store_failure_record(
    pdf_key: str,
    temp_folder: str,
    temp_files_deleted: int,
    uploader_info: dict,
    failure_reason: str,
    execution_arn: str
):
    """Store failure record in DynamoDB for daily digest."""
    try:
        table = dynamodb.Table(FAILURE_TABLE)
        now = datetime.utcnow()
        
        table.put_item(
            Item={
                'failure_id': str(uuid.uuid4()),
                'failure_date': now.strftime('%Y-%m-%d'),
                'timestamp': now.isoformat() + 'Z',
                'iam_username': uploader_info['username'],
                'user_arn': uploader_info['arn'],
                'pdf_key': pdf_key,
                'temp_folder': temp_folder,
                'temp_files_deleted': temp_files_deleted,
                'failure_reason': failure_reason,
                'execution_arn': execution_arn,
                'notified': False
            }
        )
        logger.info(f"Stored failure record for {pdf_key}")
        
    except ClientError as e:
        logger.error(f"Error storing failure record: {e}")


def log_cleanup_event(
    pdf_key: str,
    temp_folder: str,
    temp_files_deleted: int,
    uploader_info: dict,
    failure_reason: str,
    execution_arn: str
):
    """Log the cleanup event to CloudWatch."""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'event_type': 'PIPELINE_FAILURE_CLEANUP',
        'execution_arn': execution_arn,
        'failure_reason': failure_reason,
        'deleted_pdf': pdf_key,
        'deleted_temp_folder': temp_folder,
        'temp_files_deleted': temp_files_deleted,
        'uploaded_by': uploader_info['username'],
        'uploaded_by_arn': uploader_info['arn']
    }
    
    # Log to Lambda's default CloudWatch stream (parsed as JSON automatically)
    logger.info(json.dumps(log_entry))
    
    # Also log to dedicated cleanup log group
    try:
        log_stream_name = datetime.utcnow().strftime('%Y/%m/%d')
        
        try:
            logs.create_log_stream(
                logGroupName=LOG_GROUP_NAME,
                logStreamName=log_stream_name
            )
        except logs.exceptions.ResourceAlreadyExistsException:
            pass
        
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
    Lambda handler for Step Function failure events from EventBridge.
    
    Event structure:
    {
        "detail-type": "Step Functions Execution Status Change",
        "source": "aws.states",
        "detail": {
            "executionArn": "arn:aws:states:...",
            "stateMachineArn": "arn:aws:states:...",
            "status": "FAILED",
            "input": "{...}",  # JSON string of execution input
            "error": "...",
            "cause": "..."
        }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    detail = event.get('detail', {})
    execution_arn = detail.get('executionArn', 'unknown')
    status = detail.get('status', 'unknown')
    
    # Parse the execution input
    try:
        execution_input = json.loads(detail.get('input', '{}'))
    except json.JSONDecodeError:
        logger.error("Failed to parse execution input")
        execution_input = {}
    
    # Get failure reason
    failure_reason = detail.get('error', '')
    if detail.get('cause'):
        failure_reason += f": {detail.get('cause')}"
    if not failure_reason:
        failure_reason = f"Execution {status}"
    
    # Extract PDF key from execution input
    pdf_key = extract_pdf_key_from_execution(execution_input)
    if not pdf_key:
        logger.error("Could not determine PDF key from execution input")
        return {'statusCode': 400, 'body': 'Could not determine PDF key'}
    
    # Get bucket name from execution input or environment
    bucket = execution_input.get('s3_bucket', BUCKET_NAME)
    if not bucket:
        logger.error("Could not determine S3 bucket")
        return {'statusCode': 400, 'body': 'Could not determine S3 bucket'}
    
    logger.info(f"Processing failure cleanup for {pdf_key} in bucket {bucket}")
    
    # Get temp folder path
    temp_folder = get_temp_folder_path(pdf_key)
    
    # Delete the original PDF
    delete_s3_object(bucket, pdf_key)
    
    # Delete the temp folder
    temp_files_deleted = 0
    if temp_folder:
        temp_files_deleted = delete_temp_folder(bucket, temp_folder)
    
    # Get uploader info from CloudTrail
    uploader_info = get_uploader_info(bucket, pdf_key)
    logger.info(f"PDF was uploaded by: {uploader_info['username']}")
    
    # Store failure record for daily digest
    store_failure_record(
        pdf_key=pdf_key,
        temp_folder=temp_folder or '',
        temp_files_deleted=temp_files_deleted,
        uploader_info=uploader_info,
        failure_reason=failure_reason,
        execution_arn=execution_arn
    )
    
    # Log the cleanup event
    log_cleanup_event(
        pdf_key=pdf_key,
        temp_folder=temp_folder or '',
        temp_files_deleted=temp_files_deleted,
        uploader_info=uploader_info,
        failure_reason=failure_reason,
        execution_arn=execution_arn
    )
    
    logger.info(f"Cleanup complete for {pdf_key}: deleted PDF and {temp_files_deleted} temp files")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'pdf_deleted': pdf_key,
            'temp_files_deleted': temp_files_deleted,
            'uploaded_by': uploader_info['username']
        })
    }
