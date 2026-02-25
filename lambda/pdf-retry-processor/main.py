"""
PDF Queue Processor Lambda

Scheduled Lambda that manages the controlled intake of PDFs into the processing pipeline.
Handles two sources:
1. queue/ folder - Primary intake where teams upload PDFs
2. retry/ folder - Transient failures waiting for reprocessing

This prevents overwhelming AWS infrastructure (ECS throttling, subnet exhaustion)
by controlling how many PDFs enter the pipeline at once.

The in-flight tracking in the ECS containers still handles Adobe API rate limiting.
This Lambda handles the "front door" - how many Step Functions start at once.

Runs every 2 minutes via EventBridge schedule.

Configurable via SSM Parameters:
- /pdf-processing/queue-max-in-flight: Max in-flight before skipping (default: 10)
- /pdf-processing/queue-max-executions: Max running executions before skipping (default: 50)
- /pdf-processing/queue-batch-size: Files to move per invocation (default: 5)
- /pdf-processing/queue-batch-size-low-load: Files to move when load is low (default: 10)
"""

import json
import os
import time
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
ssm = boto3.client('ssm')

# Environment variables
BUCKET_NAME = os.environ.get('BUCKET_NAME', '')
RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', '')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', '')

# SSM Parameter names (can be overridden via environment variables)
SSM_MAX_IN_FLIGHT = os.environ.get('SSM_MAX_IN_FLIGHT_PARAM', '/pdf-processing/queue-max-in-flight')
SSM_MAX_EXECUTIONS = os.environ.get('SSM_MAX_EXECUTIONS_PARAM', '/pdf-processing/queue-max-executions')
SSM_BATCH_SIZE = os.environ.get('SSM_BATCH_SIZE_PARAM', '/pdf-processing/queue-batch-size')
SSM_BATCH_SIZE_LOW = os.environ.get('SSM_BATCH_SIZE_LOW_PARAM', '/pdf-processing/queue-batch-size-low-load')

# Default values (used if SSM parameters don't exist)
DEFAULT_MAX_IN_FLIGHT = 10
DEFAULT_MAX_EXECUTIONS = 50
DEFAULT_BATCH_SIZE = 5
DEFAULT_BATCH_SIZE_LOW = 10

# SSM cache
_ssm_cache = {}
_ssm_cache_time = {}
SSM_CACHE_TTL = 60  # 1 minute cache (short so changes take effect quickly)


def get_ssm_parameter(param_name: str, default: int) -> int:
    """Get an integer parameter from SSM with caching."""
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


def get_config() -> dict:
    """Get all configuration values from SSM (with defaults)."""
    return {
        'max_in_flight': get_ssm_parameter(SSM_MAX_IN_FLIGHT, DEFAULT_MAX_IN_FLIGHT),
        'max_executions': get_ssm_parameter(SSM_MAX_EXECUTIONS, DEFAULT_MAX_EXECUTIONS),
        'batch_size': get_ssm_parameter(SSM_BATCH_SIZE, DEFAULT_BATCH_SIZE),
        'batch_size_low': get_ssm_parameter(SSM_BATCH_SIZE_LOW, DEFAULT_BATCH_SIZE_LOW),
    }


def get_current_in_flight() -> int:
    """Get the current number of in-flight Adobe API requests."""
    if not RATE_LIMIT_TABLE:
        logger.warning("RATE_LIMIT_TABLE not set")
        return 999
    
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


def list_pdf_files(bucket: str, prefix: str, max_files: int = 20) -> list:
    """
    List PDF files in a folder.
    
    Returns list of dicts with 'key', 'last_modified', 'size'.
    Sorted by last_modified (oldest first - FIFO order).
    """
    pdf_files = []
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                # Only include PDF files, skip folder markers
                if key.lower().endswith('.pdf') and obj['Size'] > 0:
                    pdf_files.append({
                        'key': key,
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })
        
        # Sort by last_modified (oldest first - FIFO)
        pdf_files.sort(key=lambda x: x['last_modified'])
        
        return pdf_files[:max_files]
        
    except ClientError as e:
        logger.error(f"Error listing files in {prefix}: {e}")
        return []


def move_file_to_pdf_folder(bucket: str, source_key: str, source_prefix: str) -> str:
    """
    Move a file from source folder (queue/ or retry/) to pdf/ folder.
    
    Preserves folder structure:
        queue/collection-A/doc1.pdf -> pdf/collection-A/doc1.pdf
        retry/collection-A/doc1.pdf -> pdf/collection-A/doc1.pdf
    
    Returns the new pdf key if successful, None otherwise.
    """
    if not source_key.startswith(source_prefix):
        logger.error(f"Invalid source key format: {source_key}")
        return None
    
    # Create pdf key by replacing source prefix with 'pdf/'
    relative_path = source_key[len(source_prefix):]
    pdf_key = f"pdf/{relative_path}"
    
    try:
        # Copy to pdf folder
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=pdf_key
        )
        logger.info(f"Copied s3://{bucket}/{source_key} to s3://{bucket}/{pdf_key}")
        
        # Delete from source folder
        s3.delete_object(Bucket=bucket, Key=source_key)
        logger.info(f"Deleted s3://{bucket}/{source_key}")
        
        return pdf_key
        
    except ClientError as e:
        logger.error(f"Error moving file to pdf folder: {e}")
        return None


def handler(event, context):
    """
    Lambda handler - runs on schedule to process queue and retry files.
    
    Priority order:
    1. Check capacity (in-flight, running executions, global backoff)
    2. Process retry/ files first (they've been waiting longer)
    3. Then process queue/ files (new work)
    
    Controls intake rate to prevent overwhelming AWS infrastructure.
    
    Configuration via SSM Parameters:
    - /pdf-processing/queue-max-in-flight (default: 10)
    - /pdf-processing/queue-max-executions (default: 50)
    - /pdf-processing/queue-batch-size (default: 5)
    - /pdf-processing/queue-batch-size-low-load (default: 10)
    """
    logger.info("PDF Queue Processor starting")
    
    if not BUCKET_NAME:
        logger.error("BUCKET_NAME environment variable not set")
        return {'statusCode': 500, 'body': 'BUCKET_NAME not configured'}
    
    # Load configuration from SSM
    config = get_config()
    logger.info(f"Config: max_in_flight={config['max_in_flight']}, max_executions={config['max_executions']}, "
                f"batch_size={config['batch_size']}, batch_size_low={config['batch_size_low']}")
    
    # Check for global backoff (recent 429 errors from Adobe)
    backoff_remaining = get_global_backoff_remaining()
    if backoff_remaining > 0:
        logger.info(f"Global backoff active ({backoff_remaining}s remaining) - skipping processing")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'SKIPPED',
                'reason': f'Global backoff active ({backoff_remaining}s remaining)',
                'files_processed': 0
            })
        }
    
    # Check current capacity
    in_flight = get_current_in_flight()
    running_executions = get_running_executions_count()
    
    logger.info(f"Current capacity: {in_flight} in-flight, {running_executions} running executions")
    
    # Determine if we have capacity to process files
    if in_flight >= config['max_in_flight']:
        logger.info(f"In-flight count ({in_flight}) >= threshold ({config['max_in_flight']}) - skipping")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'SKIPPED',
                'reason': f'In-flight count ({in_flight}) above threshold',
                'files_processed': 0,
                'queue_status': {'in_flight': in_flight, 'running_executions': running_executions}
            })
        }
    
    if running_executions >= config['max_executions']:
        logger.info(f"Running executions ({running_executions}) >= threshold ({config['max_executions']}) - skipping")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'SKIPPED',
                'reason': f'Running executions ({running_executions}) above threshold',
                'files_processed': 0,
                'queue_status': {'in_flight': in_flight, 'running_executions': running_executions}
            })
        }
    
    # Determine batch size based on current load
    if running_executions < 10 and in_flight < 3:
        files_per_batch = config['batch_size_low']
        logger.info(f"Low load detected - processing up to {files_per_batch} files")
    else:
        files_per_batch = config['batch_size']
        logger.info(f"Normal load - processing up to {files_per_batch} files")
    
    processed_files = []
    files_remaining_retry = 0
    files_remaining_queue = 0
    
    # Priority 1: Process retry/ files first (they've been waiting)
    retry_files = list_pdf_files(BUCKET_NAME, 'retry/', max_files=files_per_batch)
    files_remaining_retry = len(retry_files)
    
    if retry_files:
        logger.info(f"Found {len(retry_files)} files in retry/ folder")
        
        for retry_file in retry_files:
            if len(processed_files) >= files_per_batch:
                break
            
            source_key = retry_file['key']
            pdf_key = move_file_to_pdf_folder(BUCKET_NAME, source_key, 'retry/')
            
            if pdf_key:
                processed_files.append({
                    'source': source_key,
                    'destination': pdf_key,
                    'type': 'retry',
                    'size': retry_file['size'],
                    'waited_since': retry_file['last_modified'].isoformat()
                })
                logger.info(f"Moved retry file: {source_key} -> {pdf_key}")
    
    # Priority 2: Process queue/ files if we have remaining capacity
    remaining_capacity = files_per_batch - len(processed_files)
    
    if remaining_capacity > 0:
        queue_files = list_pdf_files(BUCKET_NAME, 'queue/', max_files=remaining_capacity + 10)
        files_remaining_queue = len(queue_files)
        
        if queue_files:
            logger.info(f"Found {len(queue_files)} files in queue/ folder")
            
            for queue_file in queue_files:
                if len(processed_files) >= files_per_batch:
                    break
                
                source_key = queue_file['key']
                pdf_key = move_file_to_pdf_folder(BUCKET_NAME, source_key, 'queue/')
                
                if pdf_key:
                    processed_files.append({
                        'source': source_key,
                        'destination': pdf_key,
                        'type': 'queue',
                        'size': queue_file['size'],
                        'queued_since': queue_file['last_modified'].isoformat()
                    })
                    logger.info(f"Moved queue file: {source_key} -> {pdf_key}")
    
    # Calculate remaining files (approximate - we only fetched a limited number)
    total_remaining = max(0, files_remaining_retry - len([f for f in processed_files if f['type'] == 'retry']))
    total_remaining += max(0, files_remaining_queue - len([f for f in processed_files if f['type'] == 'queue']))
    
    # Summary
    retry_processed = len([f for f in processed_files if f['type'] == 'retry'])
    queue_processed = len([f for f in processed_files if f['type'] == 'queue'])
    
    if processed_files:
        logger.info(f"Processing complete: {retry_processed} from retry/, {queue_processed} from queue/")
    else:
        logger.info("No files to process in queue/ or retry/ folders")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'action': 'PROCESSED' if processed_files else 'NO_FILES',
            'files_processed': len(processed_files),
            'retry_processed': retry_processed,
            'queue_processed': queue_processed,
            'files_remaining_estimate': total_remaining,
            'processed_files': processed_files,
            'queue_status': {
                'in_flight': in_flight,
                'running_executions': running_executions
            }
        })
    }
