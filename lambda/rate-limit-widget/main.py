"""
Rate Limit Widget Lambda

Custom CloudWatch Dashboard widget that queries DynamoDB directly
to show the current state of the Adobe API in-flight request tracking
and RPM (requests per minute) limiting.
"""

import json
import os
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

# Counter ID for the in-flight tracker (must match rate_limiter.py)
IN_FLIGHT_COUNTER_ID = "adobe_api_in_flight"

# Counter ID prefix for RPM tracking (must match rate_limiter.py)
RPM_COUNTER_PREFIX = "rpm_window_"


def get_max_in_flight() -> int:
    """Get the max in-flight limit from SSM Parameter Store."""
    param_name = os.environ.get('ADOBE_API_MAX_IN_FLIGHT_PARAM', '/pdf-processing/adobe-api-max-in-flight')
    try:
        response = ssm.get_parameter(Name=param_name)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return 150  # Default


def get_max_rpm() -> int:
    """Get the max RPM limit from SSM Parameter Store."""
    rpm_param = os.environ.get('ADOBE_API_RPM_PARAM', '/pdf-processing/adobe-api-rpm')
    try:
        response = ssm.get_parameter(Name=rpm_param)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return 190  # Default


def get_current_rpm_count(table, api_type: str = None) -> int:
    """
    Get the current request count for this minute window.
    If api_type is provided, gets count for that specific API type.
    If api_type is None, returns combined count for both autotag and extract.
    """
    now = datetime.now(timezone.utc)
    
    if api_type:
        window_id = f"{RPM_COUNTER_PREFIX}{api_type}_{now.strftime('%Y%m%d_%H%M')}"
        try:
            response = table.get_item(
                Key={'counter_id': window_id},
                ProjectionExpression='#rc',
                ExpressionAttributeNames={'#rc': 'request_count'}
            )
            return int(response.get('Item', {}).get('request_count', 0))
        except ClientError:
            return 0
    else:
        # Get combined count for both API types
        autotag_count = get_current_rpm_count(table, 'autotag')
        extract_count = get_current_rpm_count(table, 'extract')
        return autotag_count + extract_count


def lambda_handler(event, context):
    """
    CloudWatch custom widget handler.
    
    Returns HTML content showing current in-flight request status and RPM usage.
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE', 'adobe-api-in-flight-tracker')
    
    try:
        table = dynamodb.Table(table_name)
        max_in_flight = get_max_in_flight()
        max_rpm = get_max_rpm()  # This is per API type
        
        # Get current in-flight count
        response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
        item = response.get('Item', {})
        in_flight = max(0, int(item.get('in_flight', 0)))  # Clamp to 0 minimum
        last_updated = item.get('last_updated', 'Never')
        
        # Get current RPM counts for each API type
        autotag_rpm = max(0, get_current_rpm_count(table, 'autotag'))
        extract_rpm = max(0, get_current_rpm_count(table, 'extract'))
        total_rpm = autotag_rpm + extract_rpm
        max_total_rpm = max_rpm * 2  # Combined limit for both API types
        
        available = max(0, max_in_flight - in_flight)
        
        # Calculate percentages
        pct_used = (in_flight / max_in_flight * 100) if max_in_flight > 0 else 0
        autotag_rpm_pct = (autotag_rpm / max_rpm * 100) if max_rpm > 0 else 0
        extract_rpm_pct = (extract_rpm / max_rpm * 100) if max_rpm > 0 else 0
        total_rpm_pct = (total_rpm / max_total_rpm * 100) if max_total_rpm > 0 else 0
        
        # Determine status color (based on whichever limit is closer)
        max_pct = max(pct_used, autotag_rpm_pct, extract_rpm_pct)
        if max_pct >= 90:
            status_color = '#d13212'  # Red
            status_text = 'NEAR LIMIT'
        elif max_pct >= 70:
            status_color = '#ff9900'  # Orange
            status_text = 'MODERATE'
        elif in_flight > 0 or total_rpm > 0:
            status_color = '#1d8102'  # Green
            status_text = 'PROCESSING'
        else:
            status_color = '#545b64'  # Gray
            status_text = 'IDLE'
        
        # Helper function for color based on percentage
        def get_color(pct, has_activity):
            if pct >= 90:
                return '#d13212'
            elif pct >= 70:
                return '#ff9900'
            elif has_activity:
                return '#1d8102'
            else:
                return '#545b64'
        
        inflight_color = get_color(pct_used, in_flight > 0)
        autotag_color = get_color(autotag_rpm_pct, autotag_rpm > 0)
        extract_color = get_color(extract_rpm_pct, extract_rpm > 0)
        
        # Format last updated time
        if last_updated != 'Never':
            try:
                dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                last_updated_display = dt.strftime('%H:%M:%S UTC')
            except:
                last_updated_display = last_updated
        else:
            last_updated_display = 'Never'
        
        # Current minute window for display
        now = datetime.now(timezone.utc)
        current_window = now.strftime('%H:%M')
        seconds_remaining = 60 - now.second
        
        # Build HTML response
        html = f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 10px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                <div>
                    <span style="font-size: 14px; color: #545b64;">Adobe API Rate Limiting</span>
                    <div style="font-size: 12px; color: #879596;">Last update: {last_updated_display}</div>
                </div>
                <div style="background-color: {status_color}; color: white; padding: 5px 12px; border-radius: 3px; font-weight: bold;">
                    {status_text}
                </div>
            </div>
            
            <!-- In-Flight Section -->
            <div style="margin-bottom: 20px;">
                <div style="font-size: 12px; color: #545b64; margin-bottom: 8px; font-weight: bold;">
                    Concurrent In-Flight Requests
                </div>
                <div style="display: flex; justify-content: space-around; text-align: center; margin-bottom: 8px;">
                    <div>
                        <div style="font-size: 28px; font-weight: bold; color: #16191f;">{in_flight}</div>
                        <div style="font-size: 11px; color: #545b64;">In-Flight</div>
                    </div>
                    <div style="font-size: 20px; color: #879596; align-self: center;">/</div>
                    <div>
                        <div style="font-size: 28px; font-weight: bold; color: #16191f;">{max_in_flight}</div>
                        <div style="font-size: 11px; color: #545b64;">Max</div>
                    </div>
                    <div style="font-size: 20px; color: #879596; align-self: center;">=</div>
                    <div>
                        <div style="font-size: 28px; font-weight: bold; color: {inflight_color};">{available}</div>
                        <div style="font-size: 11px; color: #545b64;">Available</div>
                    </div>
                </div>
                <div style="background-color: #eaeded; border-radius: 4px; height: 16px; overflow: hidden;">
                    <div style="background-color: {inflight_color}; height: 100%; width: {min(pct_used, 100):.1f}%;"></div>
                </div>
                <div style="text-align: center; font-size: 11px; color: #545b64; margin-top: 3px;">
                    {pct_used:.1f}% utilized
                </div>
            </div>
            
            <!-- RPM Section -->
            <div style="margin-bottom: 15px;">
                <div style="font-size: 12px; color: #545b64; margin-bottom: 8px; font-weight: bold;">
                    Requests Per Minute (Window: {current_window} UTC, {seconds_remaining}s remaining)
                </div>
                
                <!-- Autotag RPM -->
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <div style="width: 70px; font-size: 11px; color: #545b64;">Autotag:</div>
                    <div style="flex: 1; background-color: #eaeded; border-radius: 4px; height: 14px; overflow: hidden; margin-right: 10px;">
                        <div style="background-color: {autotag_color}; height: 100%; width: {min(autotag_rpm_pct, 100):.1f}%;"></div>
                    </div>
                    <div style="width: 80px; font-size: 12px; text-align: right;">
                        <span style="color: {autotag_color}; font-weight: bold;">{autotag_rpm}</span>
                        <span style="color: #545b64;">/{max_rpm}</span>
                    </div>
                </div>
                
                <!-- Extract RPM -->
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <div style="width: 70px; font-size: 11px; color: #545b64;">Extract:</div>
                    <div style="flex: 1; background-color: #eaeded; border-radius: 4px; height: 14px; overflow: hidden; margin-right: 10px;">
                        <div style="background-color: {extract_color}; height: 100%; width: {min(extract_rpm_pct, 100):.1f}%;"></div>
                    </div>
                    <div style="width: 80px; font-size: 12px; text-align: right;">
                        <span style="color: {extract_color}; font-weight: bold;">{extract_rpm}</span>
                        <span style="color: #545b64;">/{max_rpm}</span>
                    </div>
                </div>
                
                <!-- Total RPM -->
                <div style="display: flex; align-items: center; padding-top: 5px; border-top: 1px solid #eaeded;">
                    <div style="width: 70px; font-size: 11px; color: #545b64; font-weight: bold;">Total:</div>
                    <div style="flex: 1; font-size: 12px;">
                        <span style="font-weight: bold;">{total_rpm}</span>
                        <span style="color: #545b64;">/{max_total_rpm} ({total_rpm_pct:.1f}%)</span>
                    </div>
                </div>
            </div>
            
            <div style="padding: 10px; background-color: #f2f3f3; border-radius: 4px;">
                <div style="font-size: 11px; color: #545b64;">
                    <strong>Dual Protection:</strong> Tasks must pass both limits before making Adobe API calls.
                    In-flight limit ({max_in_flight}) prevents concurrent overload. RPM tracked separately per API type ({max_rpm} each, {max_total_rpm} total) to stay under Adobe's 200/min hard limit.
                </div>
            </div>
        </div>
        '''
        
        return html
        
    except Exception as e:
        return f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 20px; color: #d13212;">
            <strong>Error loading rate limit data:</strong><br/>
            {str(e)}
        </div>
        '''
