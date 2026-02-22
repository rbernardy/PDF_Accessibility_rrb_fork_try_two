"""
Adobe API Rate Limiter

This module provides rate limiting for Adobe PDF Services API calls using DynamoDB
as a distributed counter. It ensures that API calls across all concurrent ECS tasks
stay within the configured RPM (requests per minute) limit.

The rate limiter uses a token bucket approach where:
- Each minute window has a counter in DynamoDB
- Workers acquire tokens before making API calls
- If the limit is reached, workers wait until the next minute window

Configuration:
- ADOBE_API_RPM: SSM parameter containing the RPM limit (default: 200)
- RATE_LIMIT_TABLE: DynamoDB table name for rate limiting
"""

import os
import time
import logging
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

# Cache for SSM parameters
_ssm_cache = {}
_ssm_cache_time = {}
SSM_CACHE_TTL = 300  # 5 minutes


def get_ssm_parameter(param_name: str, default: str = None) -> str:
    """
    Get parameter from SSM Parameter Store with caching.
    
    Args:
        param_name: The SSM parameter name
        default: Default value if parameter not found
        
    Returns:
        The parameter value or default
    """
    current_time = time.time()
    
    # Check cache
    if param_name in _ssm_cache:
        cache_age = current_time - _ssm_cache_time.get(param_name, 0)
        if cache_age < SSM_CACHE_TTL:
            return _ssm_cache[param_name]
    
    try:
        response = ssm.get_parameter(Name=param_name)
        value = response['Parameter']['Value']
        _ssm_cache[param_name] = value
        _ssm_cache_time[param_name] = current_time
        logger.info(f"Loaded SSM parameter {param_name}: {value}")
        return value
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            logger.warning(f"SSM parameter {param_name} not found, using default: {default}")
            return default
        logger.error(f"Error getting SSM parameter {param_name}: {e}")
        return default


def get_rpm_limit() -> int:
    """
    Get the Adobe API RPM limit from SSM Parameter Store.
    
    Returns:
        The RPM limit (default: 200)
    """
    rpm_param = os.environ.get('ADOBE_API_RPM_PARAM', '/pdf-processing/adobe-api-rpm')
    rpm_str = get_ssm_parameter(rpm_param, '200')
    try:
        return int(rpm_str)
    except ValueError:
        logger.warning(f"Invalid RPM value '{rpm_str}', using default 200")
        return 200


def get_current_minute_key() -> str:
    """
    Get the current minute as a string key for DynamoDB.
    
    Returns:
        String in format 'YYYY-MM-DD-HH-MM'
    """
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%d-%H-%M')


def acquire_token(api_type: str = 'adobe_api', max_retries: int = 60) -> bool:
    """
    Attempt to acquire a rate limit token for an Adobe API call.
    
    This function will block until a token is available or max_retries is exceeded.
    It uses DynamoDB atomic counters to ensure distributed rate limiting.
    
    Args:
        api_type: The type of API call (for logging/metrics)
        max_retries: Maximum number of seconds to wait for a token
        
    Returns:
        True if token acquired, False if timed out
        
    Raises:
        Exception: If DynamoDB operations fail unexpectedly
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE')
    if not table_name:
        logger.warning("RATE_LIMIT_TABLE not set, skipping rate limiting")
        return True
    
    rpm_limit = get_rpm_limit()
    table = dynamodb.Table(table_name)
    
    for attempt in range(max_retries):
        minute_key = get_current_minute_key()
        
        try:
            # Atomic increment with condition check
            response = table.update_item(
                Key={
                    'minute_key': minute_key
                },
                UpdateExpression='SET request_count = if_not_exists(request_count, :zero) + :inc, '
                                'ttl = :ttl',
                ConditionExpression='attribute_not_exists(request_count) OR request_count < :max',
                ExpressionAttributeValues={
                    ':zero': 0,
                    ':inc': 1,
                    ':max': rpm_limit,
                    ':ttl': int(time.time()) + 120  # TTL: 2 minutes after the minute ends
                },
                ReturnValues='UPDATED_NEW'
            )
            
            new_count = response['Attributes']['request_count']
            logger.info(f"Rate limit token acquired for {api_type}: {new_count}/{rpm_limit} in minute {minute_key}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # Rate limit reached for this minute
                logger.info(f"Rate limit reached ({rpm_limit}/min), waiting for next minute window...")
                
                # Calculate time until next minute
                now = datetime.now(timezone.utc)
                seconds_until_next_minute = 60 - now.second
                
                # Wait until the next minute (with a small buffer)
                wait_time = min(seconds_until_next_minute + 1, 61)
                logger.info(f"Waiting {wait_time} seconds for next minute window...")
                time.sleep(wait_time)
                
            else:
                logger.error(f"DynamoDB error acquiring token: {e}")
                raise
    
    logger.error(f"Failed to acquire rate limit token after {max_retries} attempts")
    return False


def get_current_usage() -> dict:
    """
    Get the current rate limit usage for monitoring.
    
    Returns:
        Dictionary with current minute, count, and limit
    """
    table_name = os.environ.get('RATE_LIMIT_TABLE')
    if not table_name:
        return {'minute': 'N/A', 'count': 0, 'limit': 0, 'available': 0}
    
    rpm_limit = get_rpm_limit()
    minute_key = get_current_minute_key()
    table = dynamodb.Table(table_name)
    
    try:
        response = table.get_item(Key={'minute_key': minute_key})
        count = response.get('Item', {}).get('request_count', 0)
        return {
            'minute': minute_key,
            'count': count,
            'limit': rpm_limit,
            'available': max(0, rpm_limit - count)
        }
    except ClientError as e:
        logger.error(f"Error getting rate limit usage: {e}")
        return {'minute': minute_key, 'count': 0, 'limit': rpm_limit, 'available': rpm_limit}


class RateLimitedAPICall:
    """
    Context manager for rate-limited Adobe API calls.
    
    Usage:
        with RateLimitedAPICall('autotag') as allowed:
            if allowed:
                # Make the API call
                autotag_pdf(...)
    """
    
    def __init__(self, api_type: str = 'adobe_api'):
        self.api_type = api_type
        self.token_acquired = False
    
    def __enter__(self):
        self.token_acquired = acquire_token(self.api_type)
        return self.token_acquired
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Could add metrics/logging here for API call completion
        pass
