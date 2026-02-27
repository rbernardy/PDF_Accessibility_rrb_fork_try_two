"""
PDF Success Tracker Lambda

Triggered by EventBridge when a Step Function execution completes successfully.
Records the completion in DynamoDB for tracking throughput metrics.

Stores:
- Hourly counters (counter_id: success_hour_YYYYMMDDHH)
- Daily counters (counter_id: success_day_YYYYMMDD)
- Running totals for dashboard display

The rate-limit-widget Lambda reads these counters to display throughput metrics.
"""

import json
import os
import time
import boto3
import logging
from datetime import datetime, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', '')


def get_time_keys():
    """Get the current hour and day keys for counters."""
    now = datetime.now(timezone.utc)
    hour_key = f"success_hour_{now.strftime('%Y%m%d%H')}"
    day_key = f"success_day_{now.strftime('%Y%m%d')}"
    return hour_key, day_key, now


def increment_counter(table, counter_id: str, ttl_hours: int = 48):
    """Atomically increment a counter in DynamoDB."""
    ttl = int(time.time()) + (ttl_hours * 3600)
    
    try:
        response = table.update_item(
            Key={'counter_id': counter_id},
            UpdateExpression='SET #count = if_not_exists(#count, :zero) + :inc, #ttl = :ttl, #updated = :now',
            ExpressionAttributeNames={
                '#count': 'count',
                '#ttl': 'ttl',
                '#updated': 'updated_at'
            },
            ExpressionAttributeValues={
                ':inc': 1,
                ':zero': 0,
                ':ttl': ttl,
                ':now': datetime.now(timezone.utc).isoformat()
            },
            ReturnValues='UPDATED_NEW'
        )
        return int(response['Attributes']['count'])
    except ClientError as e:
        logger.error(f"Error incrementing counter {counter_id}: {e}")
        return -1


def handler(event, context):
    """
    Handle Step Function success events from EventBridge.
    
    Event structure:
    {
        "detail-type": "Step Functions Execution Status Change",
        "detail": {
            "executionArn": "arn:aws:states:...",
            "stateMachineArn": "arn:aws:states:...",
            "status": "SUCCEEDED",
            "input": "...",
            "output": "..."
        }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    if not RATE_LIMIT_TABLE:
        logger.error("RATE_LIMIT_TABLE not configured")
        return {'statusCode': 500, 'body': 'Configuration error'}
    
    table = dynamodb.Table(RATE_LIMIT_TABLE)
    
    # Get time-based counter keys
    hour_key, day_key, now = get_time_keys()
    
    # Extract execution details
    detail = event.get('detail', {})
    execution_arn = detail.get('executionArn', 'unknown')
    status = detail.get('status', 'unknown')
    
    if status != 'SUCCEEDED':
        logger.warning(f"Received non-success status: {status}")
        return {'statusCode': 200, 'body': 'Ignored non-success event'}
    
    # Increment hourly counter (keep for 48 hours for averaging)
    hour_count = increment_counter(table, hour_key, ttl_hours=48)
    logger.info(f"Hourly counter {hour_key}: {hour_count}")
    
    # Increment daily counter (keep for 30 days)
    day_count = increment_counter(table, day_key, ttl_hours=720)
    logger.info(f"Daily counter {day_key}: {day_count}")
    
    # Increment all-time counter (no TTL)
    try:
        response = table.update_item(
            Key={'counter_id': 'success_total'},
            UpdateExpression='SET #count = if_not_exists(#count, :zero) + :inc, #updated = :now',
            ExpressionAttributeNames={
                '#count': 'count',
                '#updated': 'updated_at'
            },
            ExpressionAttributeValues={
                ':inc': 1,
                ':zero': 0,
                ':now': now.isoformat()
            },
            ReturnValues='UPDATED_NEW'
        )
        total_count = int(response['Attributes']['count'])
        logger.info(f"Total success count: {total_count}")
    except ClientError as e:
        logger.error(f"Error updating total counter: {e}")
        total_count = -1
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'execution_arn': execution_arn,
            'hour_count': hour_count,
            'day_count': day_count,
            'total_count': total_count
        })
    }
