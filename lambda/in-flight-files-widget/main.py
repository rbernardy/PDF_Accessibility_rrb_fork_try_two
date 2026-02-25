"""
In-Flight Files Widget Lambda

Custom CloudWatch Dashboard widget that queries DynamoDB to show
the list of files currently in-flight (actively using Adobe API slots).
"""

import json
import os
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

# Prefix for individual file tracking entries (must match rate_limiter.py)
IN_FLIGHT_FILE_PREFIX = "file_"
IN_FLIGHT_COUNTER_ID = "adobe_api_in_flight"


def get_max_in_flight() -> int:
    """Get the max in-flight limit from SSM Parameter Store."""
    param_name = os.environ.get('ADOBE_API_MAX_IN_FLIGHT_PARAM', '/pdf-processing/adobe-api-max-in-flight')
    try:
        response = ssm.get_parameter(Name=param_name)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return 25  # Default (reduced from 150)


def reconcile_counter_if_needed(table, actual_file_count: int, counter_value: int) -> int:
    """
    Reconcile the in-flight counter with the actual file count.
    
    If there's a mismatch (counter > actual files), it means some containers
    crashed without releasing their slots. This function corrects the counter
    to match the actual file count.
    
    Args:
        table: DynamoDB table resource
        actual_file_count: Number of file_ entries in the table
        counter_value: Current value of the in_flight counter
        
    Returns:
        The corrected counter value
    """
    if counter_value > actual_file_count:
        # Counter is higher than actual files - containers crashed without releasing
        try:
            table.update_item(
                Key={'counter_id': IN_FLIGHT_COUNTER_ID},
                UpdateExpression='SET in_flight = :count, last_updated = :now, last_reconciled = :now',
                ExpressionAttributeValues={
                    ':count': actual_file_count,
                    ':now': datetime.now(timezone.utc).isoformat()
                }
            )
            return actual_file_count
        except ClientError as e:
            # If reconciliation fails, just return the original counter
            print(f"Failed to reconcile counter: {e}")
            return counter_value
    return counter_value


def lambda_handler(event, context):
    """
    CloudWatch custom widget handler.
    
    Returns HTML content showing the list of files currently in-flight.
    Includes automatic reconciliation of the counter with actual file entries.
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE', 'adobe-api-in-flight-tracker')
    
    try:
        table = dynamodb.Table(table_name)
        max_in_flight = get_max_in_flight()
        
        # Get list of in-flight files FIRST (to count actual entries)
        # Filter out entries that have been marked as released
        scan_response = table.scan(
            FilterExpression='begins_with(counter_id, :prefix) AND attribute_not_exists(released)',
            ExpressionAttributeValues={
                ':prefix': IN_FLIGHT_FILE_PREFIX
            }
        )
        
        files = []
        for item in scan_response.get('Items', []):
            started_at = item.get('started_at', '')
            # Calculate duration
            duration_str = ''
            if started_at:
                try:
                    start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    duration = now - start_dt
                    minutes = int(duration.total_seconds() // 60)
                    seconds = int(duration.total_seconds() % 60)
                    duration_str = f"{minutes}m {seconds}s"
                except:
                    duration_str = 'unknown'
            
            files.append({
                'filename': item.get('filename', 'unknown'),
                'api_type': item.get('api_type', 'unknown'),
                'started_at': started_at,
                'duration': duration_str
            })
        
        # Sort by started_at (oldest first)
        files.sort(key=lambda x: x['started_at'])
        
        # Get current in-flight counter value
        counter_response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
        counter_item = counter_response.get('Item', {})
        raw_counter_value = int(counter_item.get('in_flight', 0))
        
        # Reconcile counter with actual file count if mismatched
        # This fixes drift caused by crashed containers that didn't release slots
        in_flight_count = reconcile_counter_if_needed(table, len(files), raw_counter_value)
        
        # Build HTML response
        if not files:
            file_list_html = '''
            <div style="text-align: center; color: #545b64; padding: 20px;">
                No files currently in-flight
            </div>
            '''
        else:
            rows = []
            for f in files:
                api_color = '#1d8102' if f['api_type'] == 'autotag' else '#0073bb'
                rows.append(f'''
                <tr>
                    <td style="padding: 6px 10px; border-bottom: 1px solid #eaeded; font-size: 12px; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="{f['filename']}">{f['filename']}</td>
                    <td style="padding: 6px 10px; border-bottom: 1px solid #eaeded; text-align: center;">
                        <span style="background-color: {api_color}; color: white; padding: 2px 8px; border-radius: 3px; font-size: 11px;">{f['api_type']}</span>
                    </td>
                    <td style="padding: 6px 10px; border-bottom: 1px solid #eaeded; text-align: right; font-size: 12px; color: #545b64;">{f['duration']}</td>
                </tr>
                ''')
            
            file_list_html = f'''
            <div style="max-height: 300px; overflow-y: auto;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="background-color: #fafafa;">
                            <th style="padding: 8px 10px; text-align: left; font-size: 11px; color: #545b64; border-bottom: 2px solid #eaeded;">Filename</th>
                            <th style="padding: 8px 10px; text-align: center; font-size: 11px; color: #545b64; border-bottom: 2px solid #eaeded;">API Type</th>
                            <th style="padding: 8px 10px; text-align: right; font-size: 11px; color: #545b64; border-bottom: 2px solid #eaeded;">Duration</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(rows)}
                    </tbody>
                </table>
            </div>
            '''
        
        html = f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 10px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <div>
                    <span style="font-size: 14px; color: #16191f; font-weight: bold;">Files Currently In-Flight</span>
                </div>
                <div style="font-size: 12px; color: #545b64;">
                    {len(files)} files / {in_flight_count} slots used / {max_in_flight} max
                </div>
            </div>
            {file_list_html}
            <div style="margin-top: 10px; font-size: 11px; color: #879596; text-align: center;">
                Shows files actively using Adobe API slots. Auto-refreshes with dashboard.
            </div>
        </div>
        '''
        
        return html
        
    except Exception as e:
        return f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 20px; color: #d13212;">
            <strong>Error loading in-flight files:</strong><br/>
            {str(e)}
        </div>
        '''
