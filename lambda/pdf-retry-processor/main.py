"""
PDF Retry Processor Lambda

Scheduled Lambda that monitors the retry/ folder and moves PDFs back to pdf/
for reprocessing when the queue has capacity.

This handles rate-limited (429) failures by:
1. Checking current queue capacity (in-flight count, RPM usage)
2. If capacity available, moving ONE file from retry/ to pdf/
3. The S3 trigger on pdf/ will automatically start processing

Runs every 5 minutes via EventBridge schedule.
"""

import json
import os
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sfn = boto3.client('stepfunctions')

# Environment variables
BUCKET_NAME = os.environ.get('BUCKET_NAME', '')
RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', '')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', '')

# Thresholds for when to process retry files
MAX_IN_FLIGHT_THRESHOLD = 5  # Only retry when in-flight is below this
MAX_RUNNING_EXECUTIONS = 10  # Only retry when fewer than this many executions running


def get_current_in_flight() -> int:
    """Get the current number of in-flight Adobe API requests."""
    if not RATE_LIMIT_TABLE:
        logger.warning("RATE_LIMIT_TABLE not set")
        return 999  # Return high number to prevent retries
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        response = table.get_item(Key={'counter_id': 'adobe_api_in_flight'})
        return int(response.get('Item', {}).get('in_flight', 0))
    except ClientError as e:
        logger.error(f"Error getting in-flight count: {e}")
        return 999


def get_running_executions_count() -> int:
    """Get the count of currently running Step Function executions."""
    if not STATE_MACHINE_ARN:
        logger.warning("STATE_MACHINE_ARN not set")
        return 999
    
    try:
        response = sfn.list_executions(
            stateMachineArn=STATE_MACHINE_ARN,
            statusFilter='RUNNING',
            maxResults=100
        )
        count = len(response.get('executions', []))
        logger.info(f"Currently {count} running Step Function executions")
        return count
    except ClientError as e:
        logger.error(f"Error getting running executions: {e}")
        return 999


def get_global_backoff_remaining() -> int:
    """Check if there's an active global backoff period."""
    if not RATE_LIMIT_TABLE:
        return 0
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        response = table.get_item(Key={'counter_id': 'global_backoff_until'})
        item = response.get('Item')
        if not item:
            return 0
        
        import time
        backoff_until = int(item.get('backoff_until', 0))
        remaining = backoff_until - int(time.time())
        return max(0, remaining)
    except ClientError as e:
        logger.error(f"Error checking global backoff: {e}")
        return 0


def list_retry_files(bucket: str, max_files: int = 10) -> list:
    """
    List PDF files in the retry/ folder.
    
    Returns list of dicts with 'key' and 'last_modified'.
    Sorted by last_modified (oldest first) to process in FIFO order.
    """
    retry_files = []
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix='retry/'):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                if key.lower().endswith('.pdf'):
                    retry_files.append({
                        'key': key,
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })
        
        # Sort by last_modified (oldest first - FIFO)
        retry_files.sort(key=lambda x: x['last_modified'])
        
        logger.info(f"Found {len(retry_files)} files in retry/ folder")
        return retry_files[:max_files]
        
    except ClientError as e:
        logger.error(f"Error listing retry files: {e}")
        return []


def move_retry_to_pdf(bucket: str, retry_key: str) -> str:
    """
    Move a file from retry/ back to pdf/ for reprocessing.
    
    Preserves folder structure:
        retry/folder-name/document.pdf -> pdf/folder-name/document.pdf
    
    Returns the new pdf key if successful, None otherwise.
    """
    if not retry_key.startswith('retry/'):
        logger.error(f"Invalid retry key format: {retry_key}")
        return None
    
    # Create pdf key by replacing 'retry/' with 'pdf/'
    pdf_key = 'pdf/' + retry_key[6:]  # Remove 'retry/' prefix, add 'pdf/'
    
    try:
        # Copy to pdf folder
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': retry_key},
            Key=pdf_key
        )
        logger.info(f"Copied s3://{bucket}/{retry_key} to s3://{bucket}/{pdf_key}")
        
        # Delete from retry folder
        s3.delete_object(Bucket=bucket, Key=retry_key)
        logger.info(f"Deleted s3://{bucket}/{retry_key}")
        
        return pdf_key
        
    except ClientError as e:
        logger.error(f"Error moving retry file to pdf: {e}")
        return None


def handler(event, context):
    """
    Lambda handler - runs on schedule to process retry files.
    
    Only processes files when:
    1. No global backoff is active
    2. In-flight count is below threshold
    3. Running executions count is below threshold
    
    Processes ONE file at a time to avoid overwhelming the queue.
    """
    logger.info(f"PDF Retry Processor starting - checking queue capacity")
    
    if not BUCKET_NAME:
        logger.error("BUCKET_NAME environment variable not set")
        return {'statusCode': 500, 'body': 'BUCKET_NAME not configured'}
    
    # Check for global backoff (recent 429 errors)
    backoff_remaining = get_global_backoff_remaining()
    if backoff_remaining > 0:
        logger.info(f"Global backoff active ({backoff_remaining}s remaining) - skipping retry processing")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'SKIPPED',
                'reason': f'Global backoff active ({backoff_remaining}s remaining)',
                'files_processed': 0
            })
        }
    
    # Check in-flight count
    in_flight = get_current_in_flight()
    logger.info(f"Current in-flight count: {in_flight}")
    
    if in_flight >= MAX_IN_FLIGHT_THRESHOLD:
        logger.info(f"In-flight count ({in_flight}) >= threshold ({MAX_IN_FLIGHT_THRESHOLD}) - skipping retry processing")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'SKIPPED',
                'reason': f'In-flight count ({in_flight}) above threshold ({MAX_IN_FLIGHT_THRESHOLD})',
                'files_processed': 0
            })
        }
    
    # Check running executions
    running_executions = get_running_executions_count()
    
    if running_executions >= MAX_RUNNING_EXECUTIONS:
        logger.info(f"Running executions ({running_executions}) >= threshold ({MAX_RUNNING_EXECUTIONS}) - skipping retry processing")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'SKIPPED',
                'reason': f'Running executions ({running_executions}) above threshold ({MAX_RUNNING_EXECUTIONS})',
                'files_processed': 0
            })
        }
    
    # Get files from retry folder
    retry_files = list_retry_files(BUCKET_NAME, max_files=5)
    
    if not retry_files:
        logger.info("No files in retry/ folder")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'NO_FILES',
                'reason': 'No files in retry folder',
                'files_processed': 0
            })
        }
    
    # Process ONE file (conservative approach to avoid overwhelming queue)
    # We could process more if capacity is very low, but one at a time is safest
    files_to_process = 1
    if in_flight == 0 and running_executions < 5:
        # Queue is very empty - process up to 3 files
        files_to_process = min(3, len(retry_files))
    
    processed_files = []
    
    for i in range(files_to_process):
        retry_file = retry_files[i]
        retry_key = retry_file['key']
        
        logger.info(f"Processing retry file: {retry_key}")
        
        pdf_key = move_retry_to_pdf(BUCKET_NAME, retry_key)
        
        if pdf_key:
            processed_files.append({
                'retry_key': retry_key,
                'pdf_key': pdf_key,
                'size': retry_file['size'],
                'original_failure_time': retry_file['last_modified'].isoformat()
            })
            logger.info(f"Successfully moved {retry_key} to {pdf_key} for reprocessing")
        else:
            logger.error(f"Failed to move {retry_key}")
    
    remaining_files = len(retry_files) - len(processed_files)
    
    logger.info(f"Retry processing complete: {len(processed_files)} files moved, {remaining_files} remaining in retry folder")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'action': 'PROCESSED',
            'files_processed': len(processed_files),
            'files_remaining': remaining_files,
            'processed_files': processed_files,
            'queue_status': {
                'in_flight': in_flight,
                'running_executions': running_executions
            }
        })
    }
