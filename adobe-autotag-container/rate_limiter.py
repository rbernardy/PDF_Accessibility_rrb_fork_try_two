"""
Adobe API Rate Limiter with In-Flight Tracking

This module provides rate limiting for Adobe PDF Services API calls using DynamoDB
to track in-flight requests. Unlike simple per-minute counters, this tracks:
- When requests START (increment in_flight)
- When requests COMPLETE (decrement in_flight)

This ensures we never exceed the configured maximum concurrent API calls,
regardless of how fast ECS tasks spin up.

Configuration:
- ADOBE_API_MAX_IN_FLIGHT: SSM parameter for max concurrent requests (default: 150)
- RATE_LIMIT_TABLE: DynamoDB table name for rate limiting
"""

import os
import time
import logging
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

# Cache for SSM parameters
_ssm_cache = {}
_ssm_cache_time = {}
SSM_CACHE_TTL = 300  # 5 minutes

# Counter ID for the single in-flight tracker
IN_FLIGHT_COUNTER_ID = "adobe_api_in_flight"


def get_ssm_parameter(param_name: str, default: str = None) -> str:
    """
    Get parameter from SSM Parameter Store with caching.
    """
    current_time = time.time()
    
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


def get_max_in_flight() -> int:
    """
    Get the maximum allowed in-flight Adobe API requests from SSM.
    
    Default is 150 to stay safely under Adobe's 200 RPM limit,
    accounting for request duration and timing variations.
    """
    param_name = os.environ.get('ADOBE_API_MAX_IN_FLIGHT_PARAM', '/pdf-processing/adobe-api-max-in-flight')
    value_str = get_ssm_parameter(param_name, '150')
    try:
        return int(value_str)
    except ValueError:
        logger.warning(f"Invalid max_in_flight value '{value_str}', using default 150")
        return 150


def _get_table():
    """Get the DynamoDB table resource."""
    table_name = os.environ.get('RATE_LIMIT_TABLE')
    if not table_name:
        return None
    return dynamodb.Table(table_name)


def acquire_slot(api_type: str = 'adobe_api', max_wait_seconds: int = 300) -> bool:
    """
    Acquire a slot for an Adobe API call by incrementing the in-flight counter.
    
    This function will block until a slot is available or max_wait_seconds is exceeded.
    It uses DynamoDB conditional updates to ensure we never exceed max_in_flight.
    
    Args:
        api_type: The type of API call (for logging)
        max_wait_seconds: Maximum seconds to wait for a slot (default: 5 minutes)
        
    Returns:
        True if slot acquired, False if timed out
    """
    table = _get_table()
    if not table:
        logger.warning("RATE_LIMIT_TABLE not set, skipping rate limiting")
        return True
    
    max_in_flight = get_max_in_flight()
    start_time = time.time()
    attempt = 0
    
    while (time.time() - start_time) < max_wait_seconds:
        attempt += 1
        
        try:
            # Try to increment in_flight counter, but only if below max
            response = table.update_item(
                Key={'counter_id': IN_FLIGHT_COUNTER_ID},
                UpdateExpression='SET in_flight = if_not_exists(in_flight, :zero) + :inc, '
                                'last_updated = :now',
                ConditionExpression='attribute_not_exists(in_flight) OR in_flight < :max',
                ExpressionAttributeValues={
                    ':zero': 0,
                    ':inc': 1,
                    ':max': max_in_flight,
                    ':now': datetime.now(timezone.utc).isoformat()
                },
                ReturnValues='UPDATED_NEW'
            )
            
            new_count = int(response['Attributes']['in_flight'])
            logger.info(f"[{api_type}] Acquired slot: {new_count}/{max_in_flight} in-flight")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # At capacity - wait and retry
                current = get_current_in_flight()
                wait_time = min(2 + (attempt * 0.5), 10)  # Exponential backoff, max 10s
                logger.info(f"[{api_type}] At capacity ({current}/{max_in_flight}), "
                           f"waiting {wait_time:.1f}s (attempt {attempt})...")
                time.sleep(wait_time)
            else:
                logger.error(f"DynamoDB error acquiring slot: {e}")
                raise
    
    logger.error(f"[{api_type}] Failed to acquire slot after {max_wait_seconds}s")
    return False


def release_slot(api_type: str = 'adobe_api') -> bool:
    """
    Release a slot after an Adobe API call completes (success or failure).
    
    This decrements the in-flight counter. Must be called after every acquire_slot(),
    regardless of whether the API call succeeded or failed.
    
    Args:
        api_type: The type of API call (for logging)
        
    Returns:
        True if released successfully, False on error
    """
    table = _get_table()
    if not table:
        return True
    
    try:
        response = table.update_item(
            Key={'counter_id': IN_FLIGHT_COUNTER_ID},
            UpdateExpression='SET in_flight = if_not_exists(in_flight, :one) - :dec, '
                            'last_updated = :now',
            ExpressionAttributeValues={
                ':one': 1,
                ':dec': 1,
                ':now': datetime.now(timezone.utc).isoformat()
            },
            ReturnValues='UPDATED_NEW'
        )
        
        new_count = max(0, int(response['Attributes'].get('in_flight', 0)))
        logger.info(f"[{api_type}] Released slot: {new_count} now in-flight")
        return True
        
    except ClientError as e:
        logger.error(f"DynamoDB error releasing slot: {e}")
        # Don't raise - we don't want to fail the task just because we couldn't decrement
        return False


def get_current_in_flight() -> int:
    """
    Get the current number of in-flight requests.
    """
    table = _get_table()
    if not table:
        return 0
    
    try:
        response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
        return int(response.get('Item', {}).get('in_flight', 0))
    except ClientError as e:
        logger.error(f"Error getting in-flight count: {e}")
        return 0


def get_current_usage() -> dict:
    """
    Get the current rate limit usage for monitoring.
    
    Returns:
        Dictionary with in_flight count, max, and available slots
    """
    max_in_flight = get_max_in_flight()
    current = get_current_in_flight()
    
    return {
        'in_flight': current,
        'max': max_in_flight,
        'available': max(0, max_in_flight - current),
        'utilization_pct': round((current / max_in_flight) * 100, 1) if max_in_flight > 0 else 0
    }


@contextmanager
def rate_limited_call(api_type: str = 'adobe_api'):
    """
    Context manager for rate-limited Adobe API calls.
    
    Automatically acquires a slot before the call and releases it after,
    regardless of success or failure.
    
    Usage:
        with rate_limited_call('autotag') as acquired:
            if acquired:
                result = autotag_pdf(...)
            else:
                raise RuntimeError("Could not acquire rate limit slot")
    
    Or simpler:
        with rate_limited_call('autotag'):
            result = autotag_pdf(...)  # Will raise if slot not acquired
    """
    acquired = acquire_slot(api_type)
    try:
        yield acquired
    finally:
        if acquired:
            release_slot(api_type)


class RateLimitedAPICall:
    """
    Context manager class for rate-limited Adobe API calls.
    
    Usage:
        with RateLimitedAPICall('autotag') as allowed:
            if allowed:
                autotag_pdf(...)
    """
    
    def __init__(self, api_type: str = 'adobe_api'):
        self.api_type = api_type
        self.slot_acquired = False
    
    def __enter__(self):
        self.slot_acquired = acquire_slot(self.api_type)
        return self.slot_acquired
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.slot_acquired:
            release_slot(self.api_type)


# =============================================================================
# Legacy compatibility - keep old function names working
# =============================================================================

def acquire_token(api_type: str = 'adobe_api', max_retries: int = 60) -> bool:
    """
    Legacy function - now calls acquire_slot().
    Kept for backward compatibility.
    """
    return acquire_slot(api_type, max_wait_seconds=max_retries * 5)


def get_rpm_limit() -> int:
    """
    Legacy function - returns max_in_flight instead.
    Kept for backward compatibility with monitoring code.
    """
    return get_max_in_flight()
