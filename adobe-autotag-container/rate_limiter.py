"""
Adobe API Rate Limiter with In-Flight Tracking and RPM Limiting

This module provides rate limiting for Adobe PDF Services API calls using DynamoDB
to track in-flight requests AND enforce requests-per-minute limits. It tracks:
- When requests START (increment in_flight)
- When requests COMPLETE (decrement in_flight)
- How many requests started in the current minute window (RPM tracking)

This ensures we never exceed:
1. The configured maximum concurrent API calls (in-flight limit)
2. The configured requests per minute (RPM limit)

IMPORTANT: RPM is tracked as a SINGLE GLOBAL counter for all API types (autotag + extract)
because Adobe's 200 RPM limit is global, not per-API-type.

Configuration:
- ADOBE_API_MAX_IN_FLIGHT: SSM parameter for max concurrent requests (default: 150)
- ADOBE_API_RPM_PARAM: SSM parameter for max requests per minute (default: 150, global)
- RATE_LIMIT_TABLE: DynamoDB table name for rate limiting
"""

import os
import time
import logging
import boto3
import uuid
import random
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

# Counter ID prefix for RPM tracking (one per minute window)
RPM_COUNTER_PREFIX = "rpm_window_"

# Prefix for individual file tracking entries
IN_FLIGHT_FILE_PREFIX = "file_"


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
    
    Default is 50 to stay safely under Adobe's 200 RPM limit.
    Since each PDF makes 2 API calls (autotag + extract), and calls take
    ~10-30 seconds, 50 concurrent tasks could generate ~100-200 API calls/minute.
    """
    param_name = os.environ.get('ADOBE_API_MAX_IN_FLIGHT_PARAM', '/pdf-processing/adobe-api-max-in-flight')
    value_str = get_ssm_parameter(param_name, '50')
    try:
        return int(value_str)
    except ValueError:
        logger.warning(f"Invalid max_in_flight value '{value_str}', using default 50")
        return 50


def get_max_rpm() -> int:
    """
    Get the maximum allowed requests per minute from SSM.
    
    IMPORTANT: This is now a GLOBAL limit for ALL Adobe API calls combined
    (autotag + extract). Adobe's 200 RPM limit is global, not per-API-type.
    
    Default is 150 to stay safely under Adobe's 200 RPM hard limit while
    accounting for:
    - Network latency in DynamoDB updates (race conditions)
    - Multiple ECS containers checking simultaneously
    - Clock skew between containers
    
    The SSM parameter can be adjusted if needed.
    """
    param_name = os.environ.get('ADOBE_API_RPM_PARAM', '/pdf-processing/adobe-api-rpm')
    value_str = get_ssm_parameter(param_name, '150')
    try:
        return int(value_str)
    except ValueError:
        logger.warning(f"Invalid max_rpm value '{value_str}', using default 150")
        return 150


def _get_current_minute_window(api_type: str = 'adobe_api') -> str:
    """
    Get the current minute window identifier.
    Format: rpm_window_combined_YYYYMMDD_HHMM
    
    IMPORTANT: We use a SINGLE combined counter for ALL Adobe API calls
    because Adobe's 200 RPM limit is GLOBAL across all API types (autotag + extract).
    Previously we tracked them separately which could allow 95+95=190 in the same
    minute window, but if they all hit at once, Adobe would reject them.
    
    The api_type parameter is kept for logging but NOT used in the window ID.
    """
    now = datetime.now(timezone.utc)
    # Use 'combined' instead of api_type to ensure single global counter
    return f"{RPM_COUNTER_PREFIX}combined_{now.strftime('%Y%m%d_%H%M')}"


def _increment_rpm_counter(table, api_type: str = 'adobe_api') -> int:
    """
    Increment the RPM counter for the current minute window and API type.
    Returns the new count for this window.
    """
    window_id = _get_current_minute_window(api_type)
    
    try:
        response = table.update_item(
            Key={'counter_id': window_id},
            UpdateExpression='SET #rc = if_not_exists(#rc, :zero) + :inc, '
                            '#lu = :now, '
                            '#ttl = :ttl',
            ExpressionAttributeNames={
                '#rc': 'request_count',
                '#lu': 'last_updated',
                '#ttl': 'ttl'
            },
            ExpressionAttributeValues={
                ':zero': 0,
                ':inc': 1,
                ':now': datetime.now(timezone.utc).isoformat(),
                ':ttl': int(time.time()) + 120  # Auto-expire after 2 minutes
            },
            ReturnValues='UPDATED_NEW'
        )
        return int(response['Attributes']['request_count'])
    except ClientError as e:
        logger.error(f"Error incrementing RPM counter: {e}")
        return 0


def _get_current_rpm_count(table, api_type: str = 'adobe_api') -> int:
    """
    Get the current request count for this minute window and API type.
    """
    window_id = _get_current_minute_window(api_type)
    
    try:
        response = table.get_item(
            Key={'counter_id': window_id},
            ProjectionExpression='#rc',
            ExpressionAttributeNames={'#rc': 'request_count'}
        )
        return int(response.get('Item', {}).get('request_count', 0))
    except ClientError as e:
        logger.error(f"Error getting RPM count: {e}")
        return 0


def _check_rpm_limit(table, max_rpm: int, api_type: str = 'adobe_api') -> bool:
    """
    Check if we're under the RPM limit for the current minute window and API type.
    Returns True if we can proceed, False if at limit.
    """
    current_count = _get_current_rpm_count(table, api_type)
    return current_count < max_rpm


def _get_table():
    """Get the DynamoDB table resource."""
    table_name = os.environ.get('RATE_LIMIT_TABLE')
    if not table_name:
        return None
    return dynamodb.Table(table_name)


def acquire_slot(api_type: str = 'adobe_api', max_wait_seconds: int = 300, filename: str = None) -> bool:
    """
    Acquire a slot for an Adobe API call by:
    1. First checking the in-flight limit (to avoid wasting RPM slots)
    2. Then checking and incrementing the RPM counter for the current minute window
    
    This function will block until both conditions are met or max_wait_seconds is exceeded.
    It uses DynamoDB conditional updates to ensure we never exceed limits.
    
    IMPORTANT: RPM is now tracked as a SINGLE GLOBAL counter for all API types
    because Adobe's 200 RPM limit is global across autotag and extract.
    
    Args:
        api_type: The type of API call ('autotag' or 'extract') - used for logging only
        max_wait_seconds: Maximum seconds to wait for a slot (default: 5 minutes)
        filename: Optional filename to track which file is using this slot
        
    Returns:
        True if slot acquired, False if timed out
    """
    table = _get_table()
    if not table:
        logger.warning("RATE_LIMIT_TABLE not set, skipping rate limiting")
        return True
    
    max_in_flight = get_max_in_flight()
    max_rpm = get_max_rpm()
    start_time = time.time()
    attempt = 0
    
    # Add initial random jitter (0-500ms) to spread out simultaneous container starts
    initial_jitter = random.uniform(0, 0.5)
    time.sleep(initial_jitter)
    
    while (time.time() - start_time) < max_wait_seconds:
        attempt += 1
        
        # First check: In-flight limit (check this FIRST to avoid wasting RPM slots)
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
            
            in_flight_count = int(response['Attributes']['in_flight'])
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # At in-flight capacity - wait and retry
                current = get_current_in_flight()
                wait_time = min(2 + (attempt * 0.5), 10)  # Exponential backoff, max 10s
                # Add jitter to prevent thundering herd
                wait_time += random.uniform(0, 1)
                logger.info(f"[{api_type}] At in-flight capacity ({current}/{max_in_flight}), "
                           f"waiting {wait_time:.1f}s (attempt {attempt})...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"DynamoDB error checking in-flight limit: {e}")
                raise
        
        # Second check: RPM limit - now that we have an in-flight slot, check RPM
        # CRITICAL: Use combined window ID for global RPM tracking
        # If RPM fails, we need to release the in-flight slot we just acquired
        try:
            window_id = _get_current_minute_window(api_type)  # api_type ignored, uses 'combined'
            rpm_response = table.update_item(
                Key={'counter_id': window_id},
                UpdateExpression='SET #rc = if_not_exists(#rc, :zero) + :inc, '
                                '#lu = :now, '
                                '#ttl = :ttl',
                ConditionExpression='attribute_not_exists(#rc) OR #rc < :max',
                ExpressionAttributeNames={
                    '#rc': 'request_count',
                    '#lu': 'last_updated',
                    '#ttl': 'ttl'
                },
                ExpressionAttributeValues={
                    ':zero': 0,
                    ':inc': 1,
                    ':max': max_rpm,
                    ':now': datetime.now(timezone.utc).isoformat(),
                    ':ttl': int(time.time()) + 120  # Auto-expire after 2 minutes
                },
                ReturnValues='UPDATED_NEW'
            )
            rpm_count = int(rpm_response['Attributes']['request_count'])
            
            # Success! Both in-flight and RPM slots acquired
            logger.info(f"[{api_type}] Acquired slot: {in_flight_count}/{max_in_flight} in-flight, "
                       f"{rpm_count}/{max_rpm} RPM (global)")
            
            # Track individual file if filename provided
            if filename:
                _track_file_in_flight(table, filename, api_type)
            
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # At RPM limit - release the in-flight slot we just acquired
                _release_in_flight_only(table)
                
                current_rpm = _get_current_rpm_count(table, api_type)
                now = datetime.now(timezone.utc)
                seconds_until_next_minute = 60 - now.second
                # Wait until next minute window, plus small jitter
                wait_time = min(seconds_until_next_minute + 1, 15) + random.uniform(0, 2)
                logger.info(f"[{api_type}] RPM limit reached ({current_rpm}/{max_rpm} global), "
                           f"waiting {wait_time:.1f}s for next minute window (attempt {attempt})...")
                time.sleep(wait_time)
                continue
            else:
                # Unexpected error - release in-flight slot and re-raise
                _release_in_flight_only(table)
                logger.error(f"DynamoDB error checking RPM limit: {e}")
                raise
    
    logger.error(f"[{api_type}] Failed to acquire slot after {max_wait_seconds}s")
    return False


def _release_in_flight_only(table) -> bool:
    """
    Release just the in-flight counter (used when RPM check fails after in-flight succeeds).
    """
    try:
        table.update_item(
            Key={'counter_id': IN_FLIGHT_COUNTER_ID},
            UpdateExpression='SET in_flight = if_not_exists(in_flight, :one) - :dec, '
                            'last_updated = :now',
            ExpressionAttributeValues={
                ':one': 1,
                ':dec': 1,
                ':now': datetime.now(timezone.utc).isoformat()
            }
        )
        return True
    except ClientError as e:
        logger.error(f"Error releasing in-flight slot: {e}")
        return False


def _track_file_in_flight(table, filename: str, api_type: str):
    """Track an individual file as in-flight in DynamoDB."""
    try:
        # Use a unique ID to allow same file to have multiple in-flight calls
        entry_id = f"{IN_FLIGHT_FILE_PREFIX}{uuid.uuid4().hex[:8]}_{os.path.basename(filename)}"
        table.put_item(
            Item={
                'counter_id': entry_id,
                'filename': os.path.basename(filename),
                'api_type': api_type,
                'started_at': datetime.now(timezone.utc).isoformat(),
                'ttl': int(time.time()) + 3600  # Auto-expire after 1 hour as safety net
            }
        )
        logger.debug(f"Tracked file in-flight: {filename} ({api_type})")
    except ClientError as e:
        logger.warning(f"Failed to track file in-flight: {e}")


def _untrack_file_in_flight(table, filename: str, api_type: str):
    """Mark an individual file as released (no longer in-flight)."""
    try:
        # Scan for matching file entries and mark as released
        response = table.scan(
            FilterExpression='begins_with(counter_id, :prefix) AND filename = :filename AND api_type = :api_type AND attribute_not_exists(released)',
            ExpressionAttributeValues={
                ':prefix': IN_FLIGHT_FILE_PREFIX,
                ':filename': os.path.basename(filename),
                ':api_type': api_type
            }
        )
        
        if response.get('Items'):
            # Mark the first matching item as released (instead of deleting)
            item = response['Items'][0]
            table.update_item(
                Key={'counter_id': item['counter_id']},
                UpdateExpression='SET released = :released, released_at = :now',
                ExpressionAttributeValues={
                    ':released': True,
                    ':now': datetime.now(timezone.utc).isoformat()
                }
            )
            logger.info(f"Marked file as released: {filename} ({api_type})")
        else:
            logger.warning(f"No matching file entry found to untrack: {filename} ({api_type})")
    except ClientError as e:
        logger.warning(f"Failed to untrack file from in-flight: {e}")


def release_slot(api_type: str = 'adobe_api', filename: str = None) -> bool:
    """
    Release a slot after an Adobe API call completes (success or failure).
    
    This decrements the in-flight counter. Must be called after every acquire_slot(),
    regardless of whether the API call succeeded or failed.
    
    Args:
        api_type: The type of API call (for logging)
        filename: Optional filename to untrack from in-flight list
        
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
        
        # Untrack individual file if filename provided
        if filename:
            _untrack_file_in_flight(table, filename, api_type)
        
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
        Dictionary with in_flight count, max, available slots, and RPM info
    """
    table = _get_table()
    max_in_flight = get_max_in_flight()
    max_rpm = get_max_rpm()
    current_in_flight = get_current_in_flight()
    
    # Get combined RPM (single global counter for all API types)
    combined_rpm = _get_current_rpm_count(table, 'combined') if table else 0
    
    return {
        'in_flight': current_in_flight,
        'max_in_flight': max_in_flight,
        'available': max(0, max_in_flight - current_in_flight),
        'utilization_pct': round((current_in_flight / max_in_flight) * 100, 1) if max_in_flight > 0 else 0,
        'rpm_current': combined_rpm,
        'rpm_max': max_rpm,
        'rpm_utilization_pct': round((combined_rpm / max_rpm) * 100, 1) if max_rpm > 0 else 0,
        # Legacy fields for backward compatibility
        'rpm_autotag': combined_rpm,  # Deprecated: now using combined counter
        'rpm_extract': combined_rpm,  # Deprecated: now using combined counter
        'rpm_total': combined_rpm,
        'rpm_max_per_type': max_rpm,
        'rpm_max_total': max_rpm
    }


def get_in_flight_files() -> list:
    """
    Get the list of files currently in-flight (excludes released entries).
    
    Returns:
        List of dicts with filename, api_type, and started_at for each in-flight file
    """
    table = _get_table()
    if not table:
        return []
    
    try:
        response = table.scan(
            FilterExpression='begins_with(counter_id, :prefix) AND attribute_not_exists(released)',
            ExpressionAttributeValues={
                ':prefix': IN_FLIGHT_FILE_PREFIX
            }
        )
        
        files = []
        for item in response.get('Items', []):
            files.append({
                'filename': item.get('filename', 'unknown'),
                'api_type': item.get('api_type', 'unknown'),
                'started_at': item.get('started_at', '')
            })
        
        # Sort by started_at
        files.sort(key=lambda x: x['started_at'])
        return files
        
    except ClientError as e:
        logger.error(f"Error getting in-flight files: {e}")
        return []


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
    Get the configured RPM limit.
    """
    return get_max_rpm()
