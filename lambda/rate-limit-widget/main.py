"""
Rate Limit Widget Lambda

Custom CloudWatch Dashboard widget that queries DynamoDB directly
to show the current state of the Adobe API rate limit queue.
"""

import json
import os
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

def get_rpm_limit() -> int:
    """Get the Adobe API RPM limit from SSM Parameter Store."""
    rpm_param = os.environ.get('ADOBE_API_RPM_PARAM', '/pdf-processing/adobe-api-rpm')
    try:
        response = ssm.get_parameter(Name=rpm_param)
        return int(response['Parameter']['Value'])
    except (ClientError, ValueError):
        return 200  # Default

def get_current_minute_key() -> str:
    """Get the current minute as a string key for DynamoDB."""
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%d-%H-%M')

def lambda_handler(event, context):
    """
    CloudWatch custom widget handler.
    
    Returns HTML content showing current rate limit queue status.
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE', 'adobe-api-rate-limit')
    
    try:
        table = dynamodb.Table(table_name)
        rpm_limit = get_rpm_limit()
        minute_key = get_current_minute_key()
        
        # Get current minute's count
        response = table.get_item(Key={'minute_key': minute_key})
        current_count = response.get('Item', {}).get('request_count', 0)
        available = max(0, rpm_limit - current_count)
        
        # Calculate percentage used
        pct_used = (current_count / rpm_limit * 100) if rpm_limit > 0 else 0
        
        # Determine status color
        if pct_used >= 90:
            status_color = '#d13212'  # Red
            status_text = 'NEAR LIMIT'
        elif pct_used >= 70:
            status_color = '#ff9900'  # Orange
            status_text = 'MODERATE'
        else:
            status_color = '#1d8102'  # Green
            status_text = 'OK'
        
        # Build HTML response
        html = f'''
        <div style="font-family: Amazon Ember, Arial, sans-serif; padding: 10px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                <div>
                    <span style="font-size: 14px; color: #545b64;">Current Minute Window</span>
                    <div style="font-size: 12px; color: #879596;">{minute_key}</div>
                </div>
                <div style="background-color: {status_color}; color: white; padding: 5px 12px; border-radius: 3px; font-weight: bold;">
                    {status_text}
                </div>
            </div>
            
            <div style="display: flex; justify-content: space-around; text-align: center; margin-bottom: 15px;">
                <div>
                    <div style="font-size: 32px; font-weight: bold; color: #16191f;">{current_count}</div>
                    <div style="font-size: 12px; color: #545b64;">Used</div>
                </div>
                <div style="font-size: 24px; color: #879596; align-self: center;">/</div>
                <div>
                    <div style="font-size: 32px; font-weight: bold; color: #16191f;">{rpm_limit}</div>
                    <div style="font-size: 12px; color: #545b64;">Limit</div>
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
            
            <div style="margin-top: 15px; font-size: 11px; color: #879596; text-align: center;">
                Auto-refreshes with dashboard. Rate limit resets each minute.
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
