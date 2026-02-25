"""
PDF Failure Cleanup Lambda

Triggered by EventBridge when a Step Function execution fails, times out, or is aborted.

IMPORTANT: Rate limit (429) failures are handled differently:
- 429 errors are TRANSIENT - the file can be reprocessed when capacity is available
- Instead of deleting, we MOVE the PDF to a 'retry/' folder for automatic reprocessing
- A separate scheduled Lambda monitors the retry folder and reprocesses when queue has capacity

For permanent failures (corrupted PDFs, unsupported formats, etc.):
- The original PDF is deleted
- A placeholder file is created in reports/place-holders/
- A failure record is stored for the daily digest email
"""

import json
import os
import boto3
import logging
import uuid
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple
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

# ECS log group names for looking up actual errors
ADOBE_AUTOTAG_LOG_GROUP = '/ecs/pdf-remediation/adobe-autotag'
ALT_TEXT_LOG_GROUP = '/ecs/pdf-remediation/alt-text-generator'

# Patterns that indicate rate limit / transient errors (should retry, not delete)
RATE_LIMIT_PATTERNS = [
    '429',
    'Too Many Requests',
    'rate limit',
    'Rate limit',
    'throttl',
    'Throttl',
    'quota exceeded',
    'Quota exceeded',
    'Max 429 retries exceeded',  # Our own retry exhaustion message
    'RATE_LIMIT',
]


def is_rate_limit_failure(failure_reason: str, raw_cause: str) -> bool:
    """
    Check if the failure was due to rate limiting (429 errors).
    
    These failures are transient and the file should be retried, not deleted.
    
    Args:
        failure_reason: The cleaned failure reason string
        raw_cause: The raw failure cause from Step Functions
        
    Returns:
        True if this is a rate limit failure, False otherwise
    """
    # Check both the clean reason and raw cause for rate limit patterns
    combined_text = f"{failure_reason} {raw_cause}".lower()
    
    for pattern in RATE_LIMIT_PATTERNS:
        if pattern.lower() in combined_text:
            logger.info(f"Detected rate limit failure (pattern: {pattern})")
            return True
    
    return False


def move_pdf_to_retry_folder(bucket: str, pdf_key: str) -> Optional[str]:
    """
    Move a PDF from pdf/ to retry/ folder for later reprocessing.
    
    Preserves the folder structure:
        pdf/folder-name/document.pdf -> retry/folder-name/document.pdf
    
    Args:
        bucket: S3 bucket name
        pdf_key: Original PDF key (e.g., "pdf/folder-name/document.pdf")
        
    Returns:
        The new retry key if successful, None otherwise
    """
    if not pdf_key.startswith('pdf/'):
        logger.warning(f"Unexpected PDF path format for retry: {pdf_key}")
        return None
    
    # Create retry key by replacing 'pdf/' with 'retry/'
    retry_key = 'retry/' + pdf_key[4:]  # Remove 'pdf/' prefix, add 'retry/'
    
    try:
        # Copy to retry folder
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': pdf_key},
            Key=retry_key
        )
        logger.info(f"Copied s3://{bucket}/{pdf_key} to s3://{bucket}/{retry_key}")
        
        # Delete from original location
        s3.delete_object(Bucket=bucket, Key=pdf_key)
        logger.info(f"Deleted original s3://{bucket}/{pdf_key}")
        
        return retry_key
        
    except ClientError as e:
        logger.error(f"Error moving PDF to retry folder: {e}")
        return None


def extract_ecs_failure_details(failure_cause: str) -> Tuple[str, str, Optional[str]]:
    """
    Extract meaningful failure details from ECS task failure JSON.
    """
    container_name = "unknown"
    stopped_reason = "Unknown error"
    task_arn = None
    
    try:
        if "States.TaskFailed:" in failure_cause:
            json_start = failure_cause.index("States.TaskFailed:") + len("States.TaskFailed:")
            json_str = failure_cause[json_start:].strip()
        else:
            json_str = failure_cause
        
        task_data = json.loads(json_str)
        task_arn = task_data.get('TaskArn')
        stopped_reason = task_data.get('StoppedReason', 'Unknown error')
        
        containers = task_data.get('Containers', [])
        if containers:
            container = containers[0]
            container_name = container.get('Name', 'unknown')
            exit_code = container.get('ExitCode')
            if exit_code is not None and exit_code != 0:
                stopped_reason = f"{stopped_reason} (exit code: {exit_code})"
        
        task_def_arn = task_data.get('TaskDefinitionArn', '')
        if 'AltText' in task_def_arn:
            container_name = 'alt-text-generator'
        elif 'Autotag' in task_def_arn:
            container_name = 'adobe-autotag'
            
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        logger.warning(f"Could not parse ECS failure details: {e}")
    
    return container_name, stopped_reason, task_arn


def lookup_ecs_error_from_logs(container_name: str, task_arn: str, chunk_key: str) -> Optional[str]:
    """
    Look up the actual error message from ECS container logs.
    """
    if 'alt-text' in container_name.lower():
        log_group = ALT_TEXT_LOG_GROUP
    else:
        log_group = ADOBE_AUTOTAG_LOG_GROUP
    
    task_id = None
    if task_arn:
        parts = task_arn.split('/')
        if len(parts) >= 3:
            task_id = parts[-1]
    
    filename = None
    if chunk_key:
        filename = chunk_key.split('/')[-1].replace('.pdf', '')
    
    try:
        end_time = int(datetime.utcnow().timestamp() * 1000)
        start_time = end_time - (30 * 60 * 1000)
        
        filter_patterns = ['ERROR', 'Exception', 'Traceback', 'failed', 'Error:', '429']
        
        for pattern in filter_patterns:
            try:
                response = logs.filter_log_events(
                    logGroupName=log_group,
                    startTime=start_time,
                    endTime=end_time,
                    filterPattern=pattern,
                    limit=50
                )
                
                for event in response.get('events', []):
                    message = event.get('message', '')
                    log_stream = event.get('logStreamName', '')
                    
                    is_our_task = False
                    if task_id and task_id in log_stream:
                        is_our_task = True
                    elif filename and filename in message:
                        is_our_task = True
                    
                    if is_our_task:
                        if 'ERROR' in message or 'Exception' in message or 'Traceback' in message or '429' in message:
                            clean_message = message.strip()
                            if len(clean_message) > 500:
                                clean_message = clean_message[:500] + '...'
                            return clean_message
                            
            except ClientError as e:
                logger.warning(f"Error searching logs with pattern '{pattern}': {e}")
                continue
                
    except ClientError as e:
        logger.error(f"Error looking up ECS logs: {e}")
    
    return None


def build_clean_failure_reason(failure_cause: str, chunk_key: str = None) -> str:
    """
    Build a clean, human-readable failure reason from the raw failure data.
    """
    if "States.Timeout" in failure_cause:
        return "Task timed out"
    
    if "States.TaskFailed" in failure_cause:
        container_name, stopped_reason, task_arn = extract_ecs_failure_details(failure_cause)
        actual_error = lookup_ecs_error_from_logs(container_name, task_arn, chunk_key)
        
        if actual_error:
            clean_error = actual_error.replace('"', "'").replace('\\', '')
            if len(clean_error) > 200:
                clean_error = clean_error[:200] + "..."
            return f"ECS Task Failed ({container_name}): {clean_error}"
        else:
            clean_reason = stopped_reason.replace('"', "'").replace('\\', '')
            return f"ECS Task Failed ({container_name}): {clean_reason}"
    
    if "Lambda.ServiceException" in failure_cause:
        return "Lambda service error"
    
    if "Lambda.AWSLambdaException" in failure_cause:
        return "Lambda execution error"
    
    if '{"errorMessage"' in failure_cause or '"errorMessage"' in failure_cause:
        try:
            match = re.search(r'"errorMessage"\s*:\s*"([^"]+)"', failure_cause)
            if match:
                error_msg = match.group(1).replace('\\', '')
                if len(error_msg) > 200:
                    error_msg = error_msg[:200] + "..."
                return f"Error: {error_msg}"
        except Exception:
            pass
    
    clean_cause = failure_cause.replace('"', "'").replace('\\', '').replace('{', '').replace('}', '')
    if len(clean_cause) < 200:
        return clean_cause.strip()
    return clean_cause[:200].strip() + "..."


def extract_pdf_key_from_execution(execution_input: dict) -> Optional[str]:
    """Extract the original PDF S3 key from the Step Function execution input."""
    if 's3_key' in execution_input:
        return execution_input['s3_key']
    if 'pdf_key' in execution_input:
        return execution_input['pdf_key']
    if 'key' in execution_input:
        return execution_input['key']
    
    if 'chunks' in execution_input and len(execution_input['chunks']) > 0:
        chunk = execution_input['chunks'][0]
        if 'chunk_key' in chunk:
            chunk_key = chunk['chunk_key']
            parts = chunk_key.split('/')
            if len(parts) >= 3 and parts[0] == 'temp':
                folder = parts[1]
                filename = parts[2]
                return f"pdf/{folder}/{filename}.pdf"
    
    logger.warning(f"Could not extract PDF key from execution input: {execution_input}")
    return None


def get_temp_folder_path(pdf_key: str) -> Optional[str]:
    """Convert a PDF path to its corresponding temp folder path."""
    if not pdf_key.startswith('pdf/'):
        logger.warning(f"Unexpected PDF path format: {pdf_key}")
        return None
    
    relative_path = pdf_key[4:]
    if relative_path.lower().endswith('.pdf'):
        relative_path = relative_path[:-4]
    
    return f"temp/{relative_path}/"


def get_s3_object_size(bucket: str, key: str) -> Optional[int]:
    """Get the size of an S3 object in bytes."""
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return response.get('ContentLength', 0)
    except ClientError as e:
        logger.warning(f"Could not get size for s3://{bucket}/{key}: {e}")
        return None


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
    """Delete all objects under the temp folder prefix."""
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


def create_placeholder_file(
    bucket: str,
    pdf_key: str,
    file_size: Optional[int],
    page_count: Optional[int],
    failure_timestamp: str
) -> bool:
    """Create a placeholder text file in /reports/place-holders/ after a PDF is deleted."""
    try:
        parts = pdf_key.split('/')
        if len(parts) >= 3 and parts[0] == 'pdf':
            arrival_folder = parts[1]
            base_filename = os.path.splitext(parts[-1])[0]
        else:
            arrival_folder = 'unknown'
            base_filename = os.path.splitext(os.path.basename(pdf_key))[0]
        
        clean_timestamp = failure_timestamp.replace(':', '-').replace('.', '-')
        placeholder_filename = f"{arrival_folder}_{base_filename}-{clean_timestamp}.txt"
        placeholder_key = f"reports/place-holders/{placeholder_filename}"
        
        if file_size is not None:
            if file_size >= 1024 * 1024:
                size_str = f"{file_size / (1024 * 1024):.2f} MB"
            elif file_size >= 1024:
                size_str = f"{file_size / 1024:.2f} KB"
            else:
                size_str = f"{file_size} bytes"
        else:
            size_str = "unknown"
        
        page_count_str = str(page_count) if page_count is not None else "unknown"
        content = f"{pdf_key}\n{size_str}\n{page_count_str}\n{failure_timestamp}\n"
        
        s3.put_object(
            Bucket=bucket,
            Key=placeholder_key,
            Body=content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        logger.info(f"Created placeholder file: s3://{bucket}/{placeholder_key}")
        return True
        
    except ClientError as e:
        logger.error(f"Error creating placeholder file: {e}")
        return False


def get_uploader_info(bucket: str, key: str) -> dict:
    """Query CloudTrail to find who uploaded the PDF."""
    try:
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
    execution_arn: str,
    moved_to_retry: bool = False,
    retry_key: str = None
):
    """Store failure record in DynamoDB for daily digest."""
    try:
        table = dynamodb.Table(FAILURE_TABLE)
        now = datetime.utcnow()
        
        item = {
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
            'notified': False,
            'moved_to_retry': moved_to_retry,
        }
        
        if retry_key:
            item['retry_key'] = retry_key
        
        table.put_item(Item=item)
        logger.info(f"Stored failure record for {pdf_key} (moved_to_retry={moved_to_retry})")
        
    except ClientError as e:
        logger.error(f"Error storing failure record: {e}")


def log_cleanup_event(
    pdf_key: str,
    temp_folder: str,
    temp_files_deleted: int,
    uploader_info: dict,
    failure_reason: str,
    execution_arn: str,
    action: str = 'DELETED',
    retry_key: str = None
):
    """Log the cleanup event to CloudWatch."""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'event_type': 'PIPELINE_FAILURE_CLEANUP',
        'action': action,  # 'DELETED' or 'MOVED_TO_RETRY'
        'execution_arn': execution_arn,
        'failure_reason': failure_reason,
        'pdf_key': pdf_key,
        'deleted_temp_folder': temp_folder,
        'temp_files_deleted': temp_files_deleted,
        'uploaded_by': uploader_info['username'],
        'uploaded_by_arn': uploader_info['arn']
    }
    
    if retry_key:
        log_entry['retry_key'] = retry_key
    
    logger.info(json.dumps(log_entry))
    
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
        
    except ClientError as e:
        logger.error(f"Error logging to CloudWatch: {e}")


def handler(event, context):
    """
    Lambda handler for Step Function failure events from EventBridge.
    
    For rate limit (429) failures:
        - Move PDF to retry/ folder (preserving folder structure)
        - Clean up temp folder
        - Store record for tracking (marked as moved_to_retry)
        - A separate scheduler will reprocess these when queue has capacity
    
    For permanent failures:
        - Delete the original PDF
        - Create placeholder file
        - Clean up temp folder
        - Store failure record for daily digest
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
    
    # Get the chunk key for log lookup
    chunk_key = None
    if 'chunks' in execution_input and execution_input['chunks']:
        chunk_key = execution_input['chunks'][0].get('chunk_key')
    
    # Get failure reason - build a clean version
    raw_failure_reason = detail.get('error', '')
    raw_cause = detail.get('cause', '')
    if raw_cause:
        raw_failure_reason += f": {raw_cause}"
    if not raw_failure_reason:
        raw_failure_reason = f"Execution {status}"
    
    # Build clean failure reason with log lookup
    failure_reason = build_clean_failure_reason(raw_failure_reason, chunk_key)
    
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
    logger.info(f"Failure reason: {failure_reason}")
    
    # Check if this is a rate limit failure
    is_rate_limit = is_rate_limit_failure(failure_reason, raw_cause)
    
    # Get temp folder path
    temp_folder = get_temp_folder_path(pdf_key)
    
    # Get file size (for placeholder file if needed)
    file_size = get_s3_object_size(bucket, pdf_key)
    
    # Get page count from execution input if available
    page_count = execution_input.get('page_count') or execution_input.get('num_pages')
    
    # Generate failure timestamp
    failure_timestamp = datetime.utcnow().isoformat() + 'Z'
    
    # Get uploader info from CloudTrail
    uploader_info = get_uploader_info(bucket, pdf_key)
    logger.info(f"PDF was uploaded by: {uploader_info['username']}")
    
    retry_key = None
    
    if is_rate_limit:
        # RATE LIMIT FAILURE: Move to retry folder instead of deleting
        logger.info(f"Rate limit failure detected - moving {pdf_key} to retry folder")
        
        retry_key = move_pdf_to_retry_folder(bucket, pdf_key)
        
        if retry_key:
            logger.info(f"Successfully moved to {retry_key} for later reprocessing")
            action = 'MOVED_TO_RETRY'
        else:
            # Failed to move - fall back to keeping the file (don't delete)
            logger.warning(f"Failed to move to retry folder - leaving file in place")
            action = 'MOVE_FAILED_LEFT_IN_PLACE'
    else:
        # PERMANENT FAILURE: Delete the PDF and create placeholder
        logger.info(f"Permanent failure detected - deleting {pdf_key}")
        
        delete_s3_object(bucket, pdf_key)
        
        # Create placeholder file in /reports/place-holders/
        create_placeholder_file(
            bucket=bucket,
            pdf_key=pdf_key,
            file_size=file_size,
            page_count=page_count,
            failure_timestamp=failure_timestamp
        )
        action = 'DELETED'
    
    # Always clean up the temp folder (chunks, extracted data, etc.)
    temp_files_deleted = 0
    if temp_folder:
        temp_files_deleted = delete_temp_folder(bucket, temp_folder)
    
    # Store failure record for daily digest
    store_failure_record(
        pdf_key=pdf_key,
        temp_folder=temp_folder or '',
        temp_files_deleted=temp_files_deleted,
        uploader_info=uploader_info,
        failure_reason=failure_reason,
        execution_arn=execution_arn,
        moved_to_retry=is_rate_limit,
        retry_key=retry_key
    )
    
    # Log the cleanup event
    log_cleanup_event(
        pdf_key=pdf_key,
        temp_folder=temp_folder or '',
        temp_files_deleted=temp_files_deleted,
        uploader_info=uploader_info,
        failure_reason=failure_reason,
        execution_arn=execution_arn,
        action=action,
        retry_key=retry_key
    )
    
    if is_rate_limit:
        logger.info(f"Rate limit cleanup complete for {pdf_key}: moved to {retry_key}, deleted {temp_files_deleted} temp files")
    else:
        logger.info(f"Cleanup complete for {pdf_key}: deleted PDF and {temp_files_deleted} temp files")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'pdf_key': pdf_key,
            'action': action,
            'retry_key': retry_key,
            'temp_files_deleted': temp_files_deleted,
            'uploaded_by': uploader_info['username'],
            'is_rate_limit_failure': is_rate_limit
        })
    }
