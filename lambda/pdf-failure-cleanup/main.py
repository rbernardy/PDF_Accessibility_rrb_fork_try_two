"""
PDF Failure Cleanup Lambda

Triggered by EventBridge when a Step Function execution fails, times out, or is aborted.

ALL failures are handled the same way - PDFs are NEVER deleted:
- Move the PDF back to queue/ folder for automatic reprocessing
- Clean up temp folder (chunks, extracted data)
- Invoke failure analysis Lambda (generates .docx report)
- Store failure record for daily digest email

The queue processor Lambda will pick up the file again when capacity is available.
This ensures no PDFs are ever lost due to transient or permanent failures.

Failure tracking:
- Each failure increments a retry counter stored in S3 object metadata
- After MAX_RETRIES failures, original PDF moves to failed/ folder
- Failure analysis is called on EVERY failure (generates .docx report)
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

# Maximum retry attempts before moving to failed/ folder
# This prevents infinite retry loops for truly broken PDFs
# Can be overridden via SSM parameter /pdf-processing/max-retries
MAX_RETRIES_DEFAULT = int(os.environ.get('MAX_RETRIES', '3'))
SSM_MAX_RETRIES_PARAM = os.environ.get('SSM_MAX_RETRIES_PARAM', '/pdf-processing/max-retries')

# Failure analysis Lambda ARN (optional - for detailed diagnostics)
FAILURE_ANALYSIS_LAMBDA_ARN = os.environ.get('FAILURE_ANALYSIS_LAMBDA_ARN', '')

# SSM client for reading parameters
ssm = boto3.client('ssm')

# SSM cache
_ssm_cache = {}
_ssm_cache_time = {}
SSM_CACHE_TTL = 60  # 1 minute cache


def get_ssm_parameter(param_name: str, default: int) -> int:
    """Get an integer parameter from SSM with caching."""
    import time
    current_time = time.time()
    
    if param_name in _ssm_cache:
        cache_age = current_time - _ssm_cache_time.get(param_name, 0)
        if cache_age < SSM_CACHE_TTL:
            return _ssm_cache[param_name]
    
    try:
        response = ssm.get_parameter(Name=param_name)
        value = int(response['Parameter']['Value'])
        _ssm_cache[param_name] = value
        _ssm_cache_time[param_name] = current_time
        logger.info(f"Loaded SSM parameter {param_name}: {value}")
        return value
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            logger.debug(f"SSM parameter {param_name} not found, using default: {default}")
            return default
        logger.error(f"Error getting SSM parameter {param_name}: {e}")
        return default
    except ValueError:
        logger.warning(f"Invalid value for {param_name}, using default: {default}")
        return default


def get_max_retries() -> int:
    """Get the max retries value from SSM or environment."""
    return get_ssm_parameter(SSM_MAX_RETRIES_PARAM, MAX_RETRIES_DEFAULT)


def get_retry_count(bucket: str, pdf_key: str) -> int:
    """
    Get the current retry count from S3 object metadata.
    
    Returns 0 if no retry count is set (first attempt).
    """
    try:
        response = s3.head_object(Bucket=bucket, Key=pdf_key)
        metadata = response.get('Metadata', {})
        return int(metadata.get('retry-count', '0'))
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return 0
        logger.warning(f"Error getting retry count for {pdf_key}: {e}")
        return 0


def move_pdf_to_queue_folder(bucket: str, pdf_key: str, retry_count: int) -> Optional[str]:
    """
    Move a PDF from pdf/ to queue/ folder for reprocessing.
    
    Preserves the folder structure:
        pdf/folder-name/document.pdf -> queue/folder-name/document.pdf
    
    Increments the retry count in object metadata.
    
    Args:
        bucket: S3 bucket name
        pdf_key: Original PDF key (e.g., "pdf/folder-name/document.pdf")
        retry_count: Current retry count (will be incremented)
        
    Returns:
        The new queue key if successful, None otherwise
    """
    if not pdf_key.startswith('pdf/'):
        logger.warning(f"Unexpected PDF path format for queue: {pdf_key}")
        return None
    
    # Create queue key by replacing 'pdf/' with 'queue/'
    queue_key = 'queue/' + pdf_key[4:]  # Remove 'pdf/' prefix, add 'queue/'
    new_retry_count = retry_count + 1
    
    try:
        # Copy to queue folder with updated retry count in metadata
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': pdf_key},
            Key=queue_key,
            Metadata={'retry-count': str(new_retry_count)},
            MetadataDirective='REPLACE'
        )
        logger.info(f"Copied s3://{bucket}/{pdf_key} to s3://{bucket}/{queue_key} (retry #{new_retry_count})")
        
        # Delete from original location
        s3.delete_object(Bucket=bucket, Key=pdf_key)
        logger.info(f"Deleted original s3://{bucket}/{pdf_key}")
        
        return queue_key
        
    except ClientError as e:
        logger.error(f"Error moving PDF to queue folder: {e}")
        return None


def move_pdf_to_failed_folder(bucket: str, pdf_key: str, retry_count: int) -> Optional[str]:
    """
    Move a PDF from pdf/ to failed/ folder after max retries exceeded.
    
    Preserves the folder structure:
        pdf/folder-name/document.pdf -> failed/folder-name/document.pdf
    
    Args:
        bucket: S3 bucket name
        pdf_key: Original PDF key (e.g., "pdf/folder-name/document.pdf")
        retry_count: Final retry count
        
    Returns:
        The new failed key if successful, None otherwise
    """
    if not pdf_key.startswith('pdf/'):
        logger.warning(f"Unexpected PDF path format for failed: {pdf_key}")
        return None
    
    # Create failed key by replacing 'pdf/' with 'failed/'
    failed_key = 'failed/' + pdf_key[4:]  # Remove 'pdf/' prefix, add 'failed/'
    
    try:
        # Copy to failed folder with retry count in metadata
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': pdf_key},
            Key=failed_key,
            Metadata={'retry-count': str(retry_count), 'max-retries-exceeded': 'true'},
            MetadataDirective='REPLACE'
        )
        logger.info(f"Copied s3://{bucket}/{pdf_key} to s3://{bucket}/{failed_key} (max retries exceeded)")
        
        # Delete from original location
        s3.delete_object(Bucket=bucket, Key=pdf_key)
        logger.info(f"Deleted original s3://{bucket}/{pdf_key}")
        
        return failed_key
        
    except ClientError as e:
        logger.error(f"Error moving PDF to failed folder: {e}")
        return None


def move_pdf_to_retry_folder(bucket: str, pdf_key: str) -> Optional[str]:
    """
    DEPRECATED: Use move_pdf_to_queue_folder instead.
    
    Kept for backwards compatibility - redirects to queue folder.
    """
    logger.warning("move_pdf_to_retry_folder is deprecated, using move_pdf_to_queue_folder")
    retry_count = get_retry_count(bucket, pdf_key)
    return move_pdf_to_queue_folder(bucket, pdf_key, retry_count)


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
    moved_to_queue: bool = False,
    queue_key: str = None,
    retry_count: int = 0,
    max_retries_exceeded: bool = False,
    failed_key: str = None
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
            'moved_to_queue': moved_to_queue,
            'retry_count': retry_count,
            'max_retries_exceeded': max_retries_exceeded,
        }
        
        if queue_key:
            item['queue_key'] = queue_key
        if failed_key:
            item['failed_key'] = failed_key
        
        # Keep backwards compatibility
        item['moved_to_retry'] = moved_to_queue
        if queue_key:
            item['retry_key'] = queue_key
        
        table.put_item(Item=item)
        logger.info(f"Stored failure record for {pdf_key} (retry #{retry_count}, max_exceeded={max_retries_exceeded})")
        
    except ClientError as e:
        logger.error(f"Error storing failure record: {e}")


def log_cleanup_event(
    pdf_key: str,
    temp_folder: str,
    temp_files_deleted: int,
    uploader_info: dict,
    failure_reason: str,
    execution_arn: str,
    action: str = 'MOVED_TO_QUEUE',
    queue_key: str = None,
    retry_count: int = 0,
    failed_key: str = None
):
    """Log the cleanup event to CloudWatch."""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'event_type': 'PIPELINE_FAILURE_CLEANUP',
        'action': action,  # 'MOVED_TO_QUEUE', 'MOVED_TO_FAILED', or 'MOVE_FAILED_LEFT_IN_PLACE'
        'execution_arn': execution_arn,
        'failure_reason': failure_reason,
        'pdf_key': pdf_key,
        'deleted_temp_folder': temp_folder,
        'temp_files_deleted': temp_files_deleted,
        'uploaded_by': uploader_info['username'],
        'uploaded_by_arn': uploader_info['arn'],
        'retry_count': retry_count
    }
    
    if queue_key:
        log_entry['queue_key'] = queue_key
    if failed_key:
        log_entry['failed_key'] = failed_key
    
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


def invoke_failure_analysis(bucket: str, pdf_key: str, failure_reason: str, execution_arn: str):
    """
    Invoke the failure analysis Lambda for detailed diagnostics.
    
    Called on EVERY failure to generate a .docx analysis report.
    The PDF should be at the provided key location (queue/, failed/, or pdf/).
    """
    if not FAILURE_ANALYSIS_LAMBDA_ARN:
        logger.debug("Failure analysis Lambda not configured, skipping")
        return
    
    try:
        lambda_client = boto3.client('lambda')
        
        # Extract filename from key
        filename = os.path.basename(pdf_key)
        
        payload = {
            'bucket': bucket,
            'key': pdf_key,  # The failure analysis Lambda expects 'key'
            'filename': filename,
            'original_error': failure_reason,
            'api_type': 'autotag',
            'execution_arn': execution_arn
        }
        
        lambda_client.invoke(
            FunctionName=FAILURE_ANALYSIS_LAMBDA_ARN,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(payload)
        )
        logger.info(f"Invoked failure analysis Lambda for {pdf_key}")
        
    except ClientError as e:
        logger.warning(f"Failed to invoke failure analysis Lambda: {e}")


def handler(event, context):
    """
    Lambda handler for Step Function failure events from EventBridge.
    
    ALL failures are handled the same way - PDFs are NEVER deleted:
    
    1. Get retry count from S3 object metadata
    2. If retry_count < MAX_RETRIES:
       - Move PDF to queue/ folder for reprocessing
       - Increment retry count in metadata
    3. If retry_count >= MAX_RETRIES:
       - Move original PDF to failed/ folder (permanent failure after max retries)
    4. Always:
       - Clean up temp folder
       - Store failure record for daily digest
       - Invoke failure analysis Lambda for diagnostics (.docx report)
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
    
    # Get max retries from SSM (or default)
    max_retries = get_max_retries()
    logger.info(f"Max retries configured: {max_retries}")
    
    # Get current retry count
    retry_count = get_retry_count(bucket, pdf_key)
    logger.info(f"Current retry count: {retry_count}")
    
    # Get temp folder path
    temp_folder = get_temp_folder_path(pdf_key)
    
    # Get uploader info from CloudTrail
    uploader_info = get_uploader_info(bucket, pdf_key)
    logger.info(f"PDF was uploaded by: {uploader_info['username']}")
    
    queue_key = None
    failed_key = None
    max_retries_exceeded = False
    
    if retry_count >= max_retries:
        # MAX RETRIES EXCEEDED: Move to failed/ folder
        max_retries_exceeded = True
        logger.info(f"Max retries ({max_retries}) exceeded - moving {pdf_key} to failed/ folder")
        
        failed_key = move_pdf_to_failed_folder(bucket, pdf_key, retry_count)
        
        if failed_key:
            logger.info(f"Moved to {failed_key} after {retry_count} failed attempts")
            action = 'MOVED_TO_FAILED'
            # Original PDF is now in failed/ folder - no placeholder needed
        else:
            logger.warning(f"Failed to move to failed folder - leaving file in place")
            action = 'MOVE_FAILED_LEFT_IN_PLACE'
    else:
        # RETRY: Move to queue/ folder for reprocessing
        logger.info(f"Moving {pdf_key} to queue/ folder for retry #{retry_count + 1}")
        
        queue_key = move_pdf_to_queue_folder(bucket, pdf_key, retry_count)
        
        if queue_key:
            logger.info(f"Successfully moved to {queue_key} for reprocessing")
            action = 'MOVED_TO_QUEUE'
        else:
            # Failed to move - leave the file in place (don't delete)
            logger.warning(f"Failed to move to queue folder - leaving file in place")
            action = 'MOVE_FAILED_LEFT_IN_PLACE'
    
    # Always clean up the temp folder (chunks, extracted data, etc.)
    temp_files_deleted = 0
    if temp_folder:
        temp_files_deleted = delete_temp_folder(bucket, temp_folder)
    
    # Invoke failure analysis Lambda (async, for diagnostics)
    # Use the new location of the PDF (queue/ or failed/)
    analysis_key = failed_key or queue_key or pdf_key
    invoke_failure_analysis(bucket, analysis_key, failure_reason, execution_arn)
    
    # Store failure record for daily digest
    store_failure_record(
        pdf_key=pdf_key,
        temp_folder=temp_folder or '',
        temp_files_deleted=temp_files_deleted,
        uploader_info=uploader_info,
        failure_reason=failure_reason,
        execution_arn=execution_arn,
        moved_to_queue=(action == 'MOVED_TO_QUEUE'),
        queue_key=queue_key,
        retry_count=retry_count + 1,  # Increment for this failure
        max_retries_exceeded=max_retries_exceeded,
        failed_key=failed_key
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
        queue_key=queue_key,
        retry_count=retry_count + 1,
        failed_key=failed_key
    )
    
    if action == 'MOVED_TO_QUEUE':
        logger.info(f"Cleanup complete for {pdf_key}: moved to {queue_key} (retry #{retry_count + 1}), deleted {temp_files_deleted} temp files")
    elif action == 'MOVED_TO_FAILED':
        logger.info(f"Cleanup complete for {pdf_key}: moved to {failed_key} (max retries exceeded), deleted {temp_files_deleted} temp files")
    else:
        logger.info(f"Cleanup complete for {pdf_key}: left in place (move failed), deleted {temp_files_deleted} temp files")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'pdf_key': pdf_key,
            'action': action,
            'queue_key': queue_key,
            'failed_key': failed_key,
            'retry_count': retry_count + 1,
            'max_retries_exceeded': max_retries_exceeded,
            'temp_files_deleted': temp_files_deleted,
            'uploaded_by': uploader_info['username']
        })
    }
