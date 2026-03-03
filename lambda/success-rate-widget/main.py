"""
Success Rate Widget Lambda

Custom CloudWatch widget that displays PDF processing throughput metrics:
- Total PDFs processed (all time)
- PDFs processed today
- Running average per hour (last 24 hours)
- Current hour count
- Remediation goal and estimated days to completion

Reads from the success counters in the rate limit DynamoDB table.
"""

import json
import os
import boto3
import logging
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', '')
REMEDIATION_GOAL_PARAM = os.environ.get('REMEDIATION_GOAL_PARAM', '/pdf-processing/remediation-count-goal')


def get_counter(table, counter_id: str) -> int:
    """Get a counter value from DynamoDB."""
    try:
        response = table.get_item(Key={'counter_id': counter_id})
        return int(response.get('Item', {}).get('count', 0))
    except (ClientError, ValueError) as e:
        logger.error(f"Error getting counter {counter_id}: {e}")
        return 0


def get_ssm_parameter(param_name: str) -> int:
    """Get an integer value from SSM parameter."""
    try:
        response = ssm.get_parameter(Name=param_name)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError, KeyError) as e:
        logger.error(f"Error getting SSM parameter {param_name}: {e}")
        return 0


def get_hourly_counts(table, hours: int = 24) -> list:
    """Get hourly counts for the last N hours."""
    counts = []
    now = datetime.now(timezone.utc)
    
    for i in range(hours):
        hour_time = now - timedelta(hours=i)
        hour_key = f"success_hour_{hour_time.strftime('%Y%m%d%H')}"
        count = get_counter(table, hour_key)
        counts.append({
            'hour': hour_time.strftime('%Y-%m-%d %H:00'),
            'count': count
        })
    
    return counts


def handler(event, context):
    """
    CloudWatch custom widget handler.
    
    Returns HTML content for display in CloudWatch dashboard.
    """
    logger.info(f"Widget event: {json.dumps(event)}")
    
    if not RATE_LIMIT_TABLE:
        return '<div style="color: red;">RATE_LIMIT_TABLE not configured</div>'
    
    table = dynamodb.Table(RATE_LIMIT_TABLE)
    now = datetime.now(timezone.utc)
    
    # Get counters
    total_count = get_counter(table, 'success_total')
    today_key = f"success_day_{now.strftime('%Y%m%d')}"
    today_count = get_counter(table, today_key)
    current_hour_key = f"success_hour_{now.strftime('%Y%m%d%H')}"
    current_hour_count = get_counter(table, current_hour_key)
    
    # Calculate 24-hour average
    hourly_counts = get_hourly_counts(table, 24)
    total_24h = sum(h['count'] for h in hourly_counts)
    # Only count hours that have passed (not future hours)
    hours_with_data = len([h for h in hourly_counts if h['count'] > 0])
    avg_per_hour = total_24h / 24 if total_24h > 0 else 0
    
    # Calculate last 6 hours for recent trend
    last_6h = sum(h['count'] for h in hourly_counts[:6])
    avg_last_6h = last_6h / 6 if last_6h > 0 else 0
    
    # Get remediation goal and calculate estimated days
    remediation_goal = get_ssm_parameter(REMEDIATION_GOAL_PARAM)
    remaining = max(0, remediation_goal - total_count)
    if total_24h > 0:
        estimated_days = remaining / total_24h
        days_display = f"{estimated_days:.1f}"
    else:
        days_display = "N/A"
    
    # Build HTML response
    html = f'''
    <div style="font-family: Arial, sans-serif; padding: 10px;">
        <h3 style="margin: 0 0 15px 0; color: #232f3e;">PDF Processing Throughput</h3>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin-bottom: 20px;">
            <div style="background: #f0f8ff; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #0073bb;">{total_count:,}</div>
                <div style="font-size: 12px; color: #666;">Total Processed (All Time)</div>
            </div>
            <div style="background: #f0fff0; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #2e7d32;">{today_count:,}</div>
                <div style="font-size: 12px; color: #666;">Processed Today</div>
            </div>
            <div style="background: #fff8e1; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #f57c00;">{avg_per_hour:.1f}</div>
                <div style="font-size: 12px; color: #666;">Avg/Hour (24h)</div>
            </div>
            <div style="background: #fce4ec; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #c2185b;">{current_hour_count:,}</div>
                <div style="font-size: 12px; color: #666;">This Hour</div>
            </div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin-bottom: 15px;">
            <div style="background: #f5f5f5; padding: 10px; border-radius: 5px;">
                <div style="font-size: 14px; color: #333;">
                    <strong>Last 6 Hours:</strong> {last_6h:,} PDFs ({avg_last_6h:.1f}/hr avg)
                </div>
                <div style="font-size: 14px; color: #333;">
                    <strong>Last 24 Hours:</strong> {total_24h:,} PDFs
                </div>
            </div>
            <div style="background: #e8f4fd; padding: 10px; border-radius: 5px;">
                <div style="font-size: 14px; color: #333;">
                    <strong>Remediation Total Count Goal:</strong> {remediation_goal:,}
                </div>
                <div style="font-size: 14px; color: #333;">
                    <strong>Estimated Days to Completion:</strong> {days_display}
                </div>
                <div style="font-size: 10px; color: #666;">
                    (based on Last 24 Hours completion count)
                </div>
            </div>
        </div>
        
        <div style="font-size: 11px; color: #888;">
            Updated: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC
        </div>
    </div>
    '''
    
    return html
