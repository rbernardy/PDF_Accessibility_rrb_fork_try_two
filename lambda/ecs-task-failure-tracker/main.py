"""
ECS Task Failure Tracker Lambda

Triggered by EventBridge when an ECS task stops unexpectedly (crash, OOM, etc.).
Updates the corresponding file entry in adobe-api-in-flight-tracker with crash details.

This allows the failure analysis report to include accurate crash timestamps
even when the container dies before it can release its slot normally.
"""

import json
import os
import logging
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', 'adobe-api-in-flight-tracker')
ECS_CLUSTER_NAME = os.environ.get('ECS_CLUSTER_NAME', '')

# Prefix for individual file tracking entries (must match rate_limiter.py)
IN_FLIGHT_FILE_PREFIX = "file_"


def extract_task_info(event: dict) -> dict:
    """
    Extract relevant information from ECS Task State Change event.
    
    Event structure:
    {
        "detail-type": "ECS Task State Change",
        "detail": {
            "clusterArn": "arn:aws:ecs:...",
            "taskArn": "arn:aws:ecs:...",
            "lastStatus": "STOPPED",
            "stoppedAt": "2026-03-05T10:30:00.000Z",
            "stoppedReason": "Essential container in task exited",
            "stopCode": "EssentialContainerExited",
            "containers": [
                {
                    "name": "adobe-autotag",
                    "exitCode": 1,
                    "reason": "OutOfMemoryError: Container killed due to memory usage"
                }
            ],
            "overrides": {
                "containerOverrides": [
                    {
                        "name": "adobe-autotag",
                        "environment": [
                            {"name": "S3_FILE_KEY", "value": "input/file.pdf"},
                            {"name": "S3_BUCKET_NAME", "value": "bucket-name"}
                        ]
                    }
                ]
            }
        }
    }
    """
    detail = event.get('detail', {})
    
    result = {
        'task_arn': detail.get('taskArn', ''),
        'cluster_arn': detail.get('clusterArn', ''),
        'stopped_at': detail.get('stoppedAt', ''),
        'stopped_reason': detail.get('stoppedReason', ''),
        'stop_code': detail.get('stopCode', ''),
        'exit_code': None,
        'container_reason': '',
        's3_file_key': '',
        's3_bucket': '',
        'filename': ''
    }
    
    # Extract container details
    containers = detail.get('containers', [])
    for container in containers:
        if container.get('exitCode') is not None:
            result['exit_code'] = container.get('exitCode')
        if container.get('reason'):
            result['container_reason'] = container.get('reason')
    
    # Extract S3 file key from environment overrides
    overrides = detail.get('overrides', {})
    container_overrides = overrides.get('containerOverrides', [])
    for override in container_overrides:
        env_vars = override.get('environment', [])
        for env in env_vars:
            if env.get('name') == 'S3_FILE_KEY':
                result['s3_file_key'] = env.get('value', '')
            elif env.get('name') == 'S3_BUCKET_NAME':
                result['s3_bucket'] = env.get('value', '')
    
    # Extract filename from S3 key
    if result['s3_file_key']:
        result['filename'] = os.path.basename(result['s3_file_key'])
    
    return result


def is_abnormal_stop(task_info: dict) -> bool:
    """
    Determine if this task stop was abnormal (crash, OOM, error).
    
    Normal stops have exit_code=0 and no error reasons.
    """
    # Non-zero exit code indicates failure
    if task_info['exit_code'] is not None and task_info['exit_code'] != 0:
        return True
    
    # Check for error-related stop codes
    error_stop_codes = [
        'EssentialContainerExited',
        'TaskFailedToStart',
        'ServiceSchedulerInitiated',  # Could be a forced stop
        'SpotInterruption'
    ]
    if task_info['stop_code'] in error_stop_codes:
        return True
    
    # Check for error keywords in reasons
    error_keywords = ['error', 'failed', 'killed', 'oom', 'memory', 'timeout', 'crash']
    reasons = (task_info['stopped_reason'] + ' ' + task_info['container_reason']).lower()
    if any(keyword in reasons for keyword in error_keywords):
        return True
    
    return False


def update_in_flight_entry(filename: str, task_info: dict) -> bool:
    """
    Find and update the in-flight entry for this file with crash details.
    
    Searches for unreleased file entries matching the filename and updates
    them with crashed_at timestamp and crash details.
    """
    if not filename:
        logger.warning("No filename provided, cannot update in-flight entry")
        return False
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        
        # Scan for unreleased file entries matching this filename
        response = table.scan(
            FilterExpression='begins_with(counter_id, :prefix) AND filename = :filename AND attribute_not_exists(released)',
            ExpressionAttributeValues={
                ':prefix': IN_FLIGHT_FILE_PREFIX,
                ':filename': filename
            }
        )
        
        items = response.get('Items', [])
        
        if not items:
            logger.info(f"No unreleased in-flight entry found for {filename}")
            return False
        
        # Update all matching entries (usually just one)
        updated_count = 0
        for item in items:
            counter_id = item['counter_id']
            
            # Build crash details
            crash_details = {
                'stop_code': task_info['stop_code'],
                'stopped_reason': task_info['stopped_reason'],
                'container_reason': task_info['container_reason'],
                'exit_code': task_info['exit_code'],
                'task_arn': task_info['task_arn']
            }
            
            table.update_item(
                Key={'counter_id': counter_id},
                UpdateExpression='SET crashed_at = :crashed_at, '
                                'crash_details = :details, '
                                'released = :released, '
                                'released_at = :crashed_at',
                ExpressionAttributeValues={
                    ':crashed_at': task_info['stopped_at'],
                    ':details': json.dumps(crash_details),
                    ':released': True
                }
            )
            updated_count += 1
            logger.info(f"Updated in-flight entry {counter_id} with crash details")
        
        return updated_count > 0
        
    except ClientError as e:
        logger.error(f"DynamoDB error updating in-flight entry: {e}")
        return False


def decrement_in_flight_counter() -> bool:
    """
    Decrement the global in-flight counter since the task crashed without releasing.
    """
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        
        table.update_item(
            Key={'counter_id': 'adobe_api_in_flight'},
            UpdateExpression='SET in_flight = if_not_exists(in_flight, :one) - :dec, '
                            'last_updated = :now',
            ExpressionAttributeValues={
                ':one': 1,
                ':dec': 1,
                ':now': datetime.now(timezone.utc).isoformat()
            }
        )
        logger.info("Decremented in-flight counter for crashed task")
        return True
        
    except ClientError as e:
        logger.error(f"Error decrementing in-flight counter: {e}")
        return False


def handler(event, context):
    """
    Handle ECS Task State Change events from EventBridge.
    
    Only processes STOPPED tasks that appear to have crashed or failed.
    Updates the in-flight tracker with crash timestamp and details.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Verify this is a task state change event
    if event.get('detail-type') != 'ECS Task State Change':
        logger.info("Not an ECS Task State Change event, ignoring")
        return {'statusCode': 200, 'body': 'Ignored - not a task state change'}
    
    detail = event.get('detail', {})
    
    # Only process STOPPED tasks
    if detail.get('lastStatus') != 'STOPPED':
        logger.info(f"Task status is {detail.get('lastStatus')}, ignoring")
        return {'statusCode': 200, 'body': 'Ignored - task not stopped'}
    
    # Verify this is from our cluster (if configured)
    if ECS_CLUSTER_NAME:
        cluster_arn = detail.get('clusterArn', '')
        if ECS_CLUSTER_NAME not in cluster_arn:
            logger.info(f"Task from different cluster: {cluster_arn}")
            return {'statusCode': 200, 'body': 'Ignored - different cluster'}
    
    # Extract task information
    task_info = extract_task_info(event)
    logger.info(f"Task info: {json.dumps(task_info)}")
    
    # Check if this was an abnormal stop
    if not is_abnormal_stop(task_info):
        logger.info("Task stopped normally (exit code 0), no action needed")
        return {'statusCode': 200, 'body': 'Ignored - normal stop'}
    
    # Log the crash
    logger.warning(f"ECS task crashed: {task_info['filename']} - "
                   f"exit_code={task_info['exit_code']}, "
                   f"reason={task_info['stopped_reason']}")
    
    # Update the in-flight entry with crash details
    if task_info['filename']:
        updated = update_in_flight_entry(task_info['filename'], task_info)
        
        if updated:
            # Also decrement the in-flight counter since the task didn't release properly
            decrement_in_flight_counter()
    else:
        logger.warning("Could not extract filename from task, cannot update in-flight entry")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Processed ECS task failure',
            'filename': task_info['filename'],
            'stopped_at': task_info['stopped_at'],
            'stop_code': task_info['stop_code'],
            'exit_code': task_info['exit_code']
        })
    }
