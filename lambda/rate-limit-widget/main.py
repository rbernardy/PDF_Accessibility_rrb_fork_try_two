"""
Rate Limit Widget Lambda

Custom CloudWatch Dashboard widget that queries DynamoDB directly
to show the current state of the Adobe API in-flight request tracking.
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


def get_max_in_flight() -> int:
    """Get the max in-flight limit from SSM Parameter Store."""
    param_name = os.environ.get('ADOBE_API_MAX_IN_FLIGHT_PARAM', '/pdf-processing/adobe-api-max-in-flight')
    try:
        response = ssm.get_parameter(Name=param_name)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return 150  # Default


def get_rpm_reference() -> int:
    """Get Adobe's actual RPM limit for reference display."""
    rpm_param = os.environ.get('ADOBE_API_RPM_PARAM', '/pdf-processing/adobe-api-rpm')
    try:
        response = ssm.get_parameter(Name=rpm_param)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return 200  # Default


def lambda_handler(event, context):
    """
    CloudWatch custom widget handler.
    
    Returns HTML content showing current in-flight request status.
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE', 'adobe-api-in-flight-tracker')
    
    try:
        table = dynamodb.Table(table_name)
        max_in_flight = get_max_in_flight()
        rpm_reference = get_rpm_reference()
        
        # Get current in-flight count
        response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
        item = response.get('Item', {})
        in_flight = int(item.get('in_flight', 0))
        last_updated = item.get('last_updated', 'Never')
        
        available = max(0, max_in_flight - in_flight)
        
        # Calculate percentage used
        pct_used = (in_flight / max_in_flight * 100) if max_in_flight > 0 else 0
        
        # Determine status color
        if pct_used >= 90:
            status_color = '#d13212'  # Red
            status_text = 'NEAR LIMIT'
        elif pct_used >= 70:
            status_color = '#ff9900'  # Orange
            status_text = 'MODERATE'
        elif in_flight > 0:
            status_color = '#1d8102'  # Green
            status_text = 'PROCESSING'
        else:
            status_color = '#545b64'  # Gray
            status_text = 'IDLE'
        
        # Format last updated time
        if last_updated != 'Never':
            try:
                # Parse ISO format and make it more readable
                dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                last_updated_display = dt.strftime('%H:%M:%S UTC')
            except:
                last_updated_display = last_updated
        else:
            last_updated_display = 'Never'
        
        # Build HTML response
        html = f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 10px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                <div>
                    <span style="font-size: 14px; color: #545b64;">In-Flight API Requests</span>
                    <div style="font-size: 12px; color: #879596;">Last update: {last_updated_display}</div>
                </div>
                <div style="background-color: {status_color}; color: white; padding: 5px 12px; border-radius: 3px; font-weight: bold;">
                    {status_text}
                </div>
            </div>
            
            <div style="display: flex; justify-content: space-around; text-align: center; margin-bottom: 15px;">
                <div>
                    <div style="font-size: 32px; font-weight: bold; color: #16191f;">{in_flight}</div>
                    <div style="font-size: 12px; color: #545b64;">In-Flight</div>
                </div>
                <div style="font-size: 24px; color: #879596; align-self: center;">/</div>
                <div>
                    <div style="font-size: 32px; font-weight: bold; color: #16191f;">{max_in_flight}</div>
                    <div style="font-size: 12px; color: #545b64;">Max Allowed</div>
                </div>
                <div style="font-size: 24px; color: #879596; align-self: center;">=</div>
                <div>
                    <div style="font-size: 32px; font-weight: bold; color: {status_color};">{available}</div>
                    <div style="font-size: 12px; color: #545b64;">Available</div>
                </div>
            </div>
            
            <div style="background-color: #eaeded; border-radius: 4px; height: 20px; overflow: hidden;">
                <div style="background-color: {status_color}; height: 100%; width: {min(pct_used, 100):.1f}%; transition: width 0.3s;"></div>
            </div>
            <div style="text-align: center; font-size: 12px; color: #545b64; margin-top: 5px;">
                {pct_used:.1f}% utilized
            </div>
            
            <div style="margin-top: 15px; padding: 10px; background-color: #f2f3f3; border-radius: 4px;">
                <div style="font-size: 11px; color: #545b64;">
                    <strong>How it works:</strong> Tasks acquire slots before Adobe API calls and release them on completion.
                    Max in-flight ({max_in_flight}) is set below Adobe's {rpm_reference} RPM limit to prevent 429 errors.
                </div>
            </div>
            
            <div style="margin-top: 10px; font-size: 11px; color: #879596; text-align: center;">
                Auto-refreshes with dashboard. Counter updates in real-time as API calls start/complete.
            </div>
        </div>
        '''
        
        return html
        
    except Exception as e:
        return f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 20px; color: #d13212;">
            <strong>Error loading in-flight data:</strong><br/>
            {str(e)}
        </div>
        '''
