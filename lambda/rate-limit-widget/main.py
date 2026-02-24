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
        return 150  # Default - global limit for all API types combined


def get_current_rpm_count(table, api_type: str = None) -> int:
    """
    Get the current request count for this minute window.
    
    IMPORTANT: We now use a SINGLE combined counter for all API types
    because Adobe's 200 RPM limit is global across autotag and extract.
    
    The api_type parameter is kept for backward compatibility but is ignored.
    """
    now = datetime.now(timezone.utc)
    
    # Always use 'combined' window ID - Adobe's limit is global
    window_id = f"{RPM_COUNTER_PREFIX}combined_{now.strftime('%Y%m%d_%H%M')}"
    try:
        response = table.get_item(
            Key={'counter_id': window_id},
            ProjectionExpression='#rc',
            ExpressionAttributeNames={'#rc': 'request_count'}
        )
        return int(response.get('Item', {}).get('request_count', 0))
    except ClientError:
        return 0


def lambda_handler(event, context):
    """
    CloudWatch custom widget handler.
    
    Returns HTML content showing current in-flight request status and RPM usage.
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE', 'adobe-api-in-flight-tracker')
    
    try:
        table = dynamodb.Table(table_name)
        max_in_flight = get_max_in_flight()
        max_rpm = get_max_rpm()  # Global limit for all API types combined
        
        # Get current in-flight count
        response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
        item = response.get('Item', {})
        in_flight = max(0, int(item.get('in_flight', 0)))  # Clamp to 0 minimum
        last_updated = item.get('last_updated', 'Never')
        
        # Get current RPM count (combined for all API types)
        current_rpm = max(0, get_current_rpm_count(table))
        
        available = max(0, max_in_flight - in_flight)
        
        # Calculate percentages
        pct_used = (in_flight / max_in_flight * 100) if max_in_flight > 0 else 0
        rpm_pct = (current_rpm / max_rpm * 100) if max_rpm > 0 else 0
        
        # Determine status color (based on whichever limit is closer)
        max_pct = max(pct_used, rpm_pct)
        if max_pct >= 90:
            status_color = '#d13212'  # Red
            status_text = 'NEAR LIMIT'
        elif max_pct >= 70:
            status_color = '#ff9900'  # Orange
            status_text = 'MODERATE'
        elif in_flight > 0 or current_rpm > 0:
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
        rpm_color = get_color(rpm_pct, current_rpm > 0)
        
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
        
        # Calculate RPM available
        rpm_available = max(0, max_rpm - current_rpm)
        
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
                    Requests Per Minute - Global (Window: {current_window} UTC, {seconds_remaining}s remaining)
                </div>
                <div style="display: flex; justify-content: space-around; text-align: center; margin-bottom: 8px;">
                    <div>
                        <div style="font-size: 28px; font-weight: bold; color: {rpm_color};">{current_rpm}</div>
                        <div style="font-size: 11px; color: #545b64;">This Minute</div>
                    </div>
                    <div style="font-size: 20px; color: #879596; align-self: center;">/</div>
                    <div>
                        <div style="font-size: 28px; font-weight: bold; color: #16191f;">{max_rpm}</div>
                        <div style="font-size: 11px; color: #545b64;">Max RPM</div>
                    </div>
                    <div style="font-size: 20px; color: #879596; align-self: center;">=</div>
                    <div>
                        <div style="font-size: 28px; font-weight: bold; color: {rpm_color};">{rpm_available}</div>
                        <div style="font-size: 11px; color: #545b64;">Available</div>
                    </div>
                </div>
                <div style="background-color: #eaeded; border-radius: 4px; height: 16px; overflow: hidden;">
                    <div style="background-color: {rpm_color}; height: 100%; width: {min(rpm_pct, 100):.1f}%;"></div>
                </div>
                <div style="text-align: center; font-size: 11px; color: #545b64; margin-top: 3px;">
                    {rpm_pct:.1f}% of Adobe's 200 RPM limit (using {max_rpm} as safety margin)
                </div>
            </div>
            
            <div style="padding: 10px; background-color: #f2f3f3; border-radius: 4px;">
                <div style="font-size: 11px; color: #545b64;">
                    <strong>Dual Protection:</strong> Tasks must pass both limits before making Adobe API calls.
                    In-flight limit ({max_in_flight}) prevents concurrent overload. 
                    RPM limit ({max_rpm}/min) is global for all API types combined to stay safely under Adobe's 200/min hard limit.
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
