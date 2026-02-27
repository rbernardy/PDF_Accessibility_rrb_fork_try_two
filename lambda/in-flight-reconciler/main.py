"""
In-Flight Counter Reconciler Lambda

Scheduled Lambda that prevents the in-flight counter from getting stuck by:
1. Comparing the counter value against actual running ECS tasks
2. Comparing against tracked file entries (which have TTL)
3. Resetting the counter if it's clearly stale

This handles cases where ECS tasks crash without releasing their slots.

Runs every 5 minutes via EventBridge schedule.

Configuration via SSM Parameters:
- /pdf-processing/reconciler-enabled: Enable/disable reconciliation (default: true)
- /pdf-processing/reconciler-max-drift: Max allowed drift before reset (default: 5)
"""

import json
import os
import boto3
import logging
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
ecs = boto3.client('ecs')
sfn = boto3.client('stepfunctions')
ssm = boto3.client('ssm')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', '')
ECS_CLUSTER = os.environ.get('ECS_CLUSTER', '')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', '')

# Counter IDs (must match rate_limiter.py)
IN_FLIGHT_COUNTER_ID = "adobe_api_in_flight"
IN_FLIGHT_FILE_PREFIX = "file_"

# SSM parameters
SSM_ENABLED = '/pdf-processing/reconciler-enabled'
SSM_MAX_DRIFT = '/pdf-processing/reconciler-max-drift'

# Defaults
DEFAULT_MAX_DRIFT = 5  # Reset if counter exceeds actual by more than this


def get_ssm_bool(param_name: str, default: bool) -> bool:
    """Get boolean parameter from SSM."""
    try:
        response = ssm.get_parameter(Name=param_name)
        return response['Parameter']['Value'].lower() in ('true', '1', 'yes', 'on')
    except ClientError:
        return default


def get_ssm_int(param_name: str, default: int) -> int:
    """Get integer parameter from SSM."""
    try:
        response = ssm.get_parameter(Name=param_name)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return default


def get_current_counter_value() -> int:
    """Get the current in-flight counter value from DynamoDB."""
    if not RATE_LIMIT_TABLE:
        return 0
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
        return int(response.get('Item', {}).get('in_flight', 0))
    except ClientError as e:
        logger.error(f"Error getting counter value: {e}")
        return 0


def get_tracked_files_count() -> int:
    """Count file entries that haven't been released (and haven't expired via TTL)."""
    if not RATE_LIMIT_TABLE:
        return 0
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        response = table.scan(
            FilterExpression='begins_with(counter_id, :prefix) AND attribute_not_exists(released)',
            ExpressionAttributeValues={':prefix': IN_FLIGHT_FILE_PREFIX},
            Select='COUNT'
        )
        return response.get('Count', 0)
    except ClientError as e:
        logger.error(f"Error counting tracked files: {e}")
        return 0


def get_running_ecs_tasks() -> int:
    """Count running ECS tasks in the PDF processing cluster."""
    if not ECS_CLUSTER:
        return -1  # Unknown
    
    try:
        # List running tasks
        response = ecs.list_tasks(
            cluster=ECS_CLUSTER,
            desiredStatus='RUNNING',
            maxResults=100
        )
        return len(response.get('taskArns', []))
    except ClientError as e:
        logger.error(f"Error listing ECS tasks: {e}")
        return -1


def get_running_step_functions() -> int:
    """Count running Step Function executions."""
    if not STATE_MACHINE_ARN:
        return -1
    
    try:
        response = sfn.list_executions(
            stateMachineArn=STATE_MACHINE_ARN,
            statusFilter='RUNNING',
            maxResults=100
        )
        return len(response.get('executions', []))
    except ClientError as e:
        logger.error(f"Error listing Step Functions: {e}")
        return -1


def reset_counter(new_value: int, reason: str) -> bool:
    """Reset the in-flight counter to a specific value."""
    if not RATE_LIMIT_TABLE:
        return False
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        table.update_item(
            Key={'counter_id': IN_FLIGHT_COUNTER_ID},
            UpdateExpression='SET in_flight = :val, last_updated = :now, last_reconciled = :now, reconcile_reason = :reason',
            ExpressionAttributeValues={
                ':val': new_value,
                ':now': datetime.now(timezone.utc).isoformat(),
                ':reason': reason
            }
        )
        logger.info(f"Reset counter to {new_value}: {reason}")
        return True
    except ClientError as e:
        logger.error(f"Error resetting counter: {e}")
        return False


def cleanup_stale_file_entries() -> int:
    """Remove file entries that are clearly stale (started > 15 minutes ago and not released).
    
    Adobe API calls typically complete in 20-60 seconds. If an entry is 15+ minutes old,
    the container almost certainly crashed without releasing the slot.
    """
    if not RATE_LIMIT_TABLE:
        return 0
    
    try:
        table = dynamodb.Table(RATE_LIMIT_TABLE)
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        
        # Find stale entries
        response = table.scan(
            FilterExpression='begins_with(counter_id, :prefix) AND attribute_not_exists(released) AND started_at < :cutoff',
            ExpressionAttributeValues={
                ':prefix': IN_FLIGHT_FILE_PREFIX,
                ':cutoff': cutoff
            }
        )
        
        stale_count = 0
        for item in response.get('Items', []):
            # Mark as released (stale)
            table.update_item(
                Key={'counter_id': item['counter_id']},
                UpdateExpression='SET released = :released, released_at = :now, stale_cleanup = :stale',
                ExpressionAttributeValues={
                    ':released': True,
                    ':now': datetime.now(timezone.utc).isoformat(),
                    ':stale': True
                }
            )
            stale_count += 1
            logger.info(f"Marked stale file entry: {item.get('filename', 'unknown')}")
        
        return stale_count
    except ClientError as e:
        logger.error(f"Error cleaning stale entries: {e}")
        return 0


def publish_metric(metric_name: str, value: float, unit: str = 'Count'):
    """Publish a custom CloudWatch metric."""
    try:
        cloudwatch.put_metric_data(
            Namespace='PDF-Processing/RateLimiting',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.now(timezone.utc)
            }]
        )
    except ClientError as e:
        logger.warning(f"Failed to publish metric {metric_name}: {e}")


def handler(event, context):
    """
    Reconcile the in-flight counter with actual system state.
    
    Logic:
    1. If counter > 0 but no ECS tasks running and no Step Functions running → reset to 0
    2. If counter > tracked_files + max_drift → reset to tracked_files count
    3. Clean up stale file entries (> 2 hours old)
    """
    logger.info("In-flight reconciler starting")
    
    # Check if enabled
    if not get_ssm_bool(SSM_ENABLED, True):
        logger.info("Reconciler disabled via SSM parameter")
        return {'statusCode': 200, 'body': 'Disabled'}
    
    max_drift = get_ssm_int(SSM_MAX_DRIFT, DEFAULT_MAX_DRIFT)
    
    # Gather current state
    counter_value = get_current_counter_value()
    tracked_files = get_tracked_files_count()
    running_ecs = get_running_ecs_tasks()
    running_sfn = get_running_step_functions()
    
    logger.info(f"Current state: counter={counter_value}, tracked_files={tracked_files}, "
                f"ecs_tasks={running_ecs}, step_functions={running_sfn}")
    
    # Publish metrics
    publish_metric('InFlightCounter', counter_value)
    publish_metric('TrackedFiles', tracked_files)
    if running_ecs >= 0:
        publish_metric('RunningECSTasks', running_ecs)
    if running_sfn >= 0:
        publish_metric('RunningStepFunctions', running_sfn)
    
    action_taken = 'NONE'
    reset_to = None
    reason = None
    
    # Check for clearly stuck counter
    if counter_value > 0:
        # Case 1: Counter > 0 but nothing is actually running
        if running_ecs == 0 and running_sfn == 0:
            reason = f"Counter={counter_value} but no ECS tasks or Step Functions running"
            reset_to = 0
            action_taken = 'RESET_TO_ZERO'
        
        # Case 2: Counter significantly exceeds tracked files
        elif counter_value > tracked_files + max_drift:
            reason = f"Counter={counter_value} exceeds tracked_files={tracked_files} by more than {max_drift}"
            reset_to = max(0, tracked_files)
            action_taken = 'RESET_TO_TRACKED'
        
        # Case 3: Counter is negative (shouldn't happen but let's handle it)
        elif counter_value < 0:
            reason = f"Counter={counter_value} is negative"
            reset_to = 0
            action_taken = 'RESET_NEGATIVE'
    
    # Perform reset if needed
    if reset_to is not None:
        logger.warning(f"Reconciliation needed: {reason}")
        if reset_counter(reset_to, reason):
            publish_metric('ReconciliationResets', 1)
            logger.info(f"Counter reset from {counter_value} to {reset_to}")
        else:
            action_taken = 'RESET_FAILED'
    
    # Clean up stale file entries
    stale_cleaned = cleanup_stale_file_entries()
    if stale_cleaned > 0:
        logger.info(f"Cleaned up {stale_cleaned} stale file entries")
        publish_metric('StaleEntriesCleaned', stale_cleaned)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'action': action_taken,
            'counter_before': counter_value,
            'counter_after': reset_to if reset_to is not None else counter_value,
            'tracked_files': tracked_files,
            'running_ecs': running_ecs,
            'running_sfn': running_sfn,
            'stale_entries_cleaned': stale_cleaned,
            'reason': reason
        })
    }
