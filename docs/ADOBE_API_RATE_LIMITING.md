# Adobe API Rate Limiting Implementation

## Overview

This document describes the distributed rate limiting implementation for Adobe PDF Services API calls. The rate limiter ensures that all concurrent ECS tasks stay within the configured RPM (requests per minute) limit.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ECS Task (Adobe Autotag)                          │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │  rate_limiter   │───▶│  DynamoDB       │───▶│  Adobe API Call         │  │
│  │  acquire_token()│    │  (atomic counter)│    │  (autotag/extract)      │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘  │
│          │                      ▲                                           │
│          │                      │                                           │
│          ▼                      │                                           │
│  ┌─────────────────┐            │                                           │
│  │  SSM Parameter  │────────────┘                                           │
│  │  (RPM limit)    │                                                        │
│  └─────────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. DynamoDB Rate Limit Table

- **Table Name**: `adobe-api-rate-limit`
- **Partition Key**: `minute_key` (string, format: `YYYY-MM-DD-HH-MM`)
- **Attributes**:
  - `request_count`: Atomic counter for API calls in this minute
  - `ttl`: Time-to-live for automatic cleanup (2 minutes after the minute ends)

### 2. SSM Parameter

- **Parameter Name**: `/pdf-processing/adobe-api-rpm`
- **Default Value**: `200`
- **Description**: Adobe PDF Services API rate limit (requests per minute)

### 3. Rate Limiter Module

Located at: `adobe-autotag-container/rate_limiter.py`

Key functions:
- `acquire_token(api_type)`: Attempts to acquire a rate limit token, blocks if limit reached
- `get_current_usage()`: Returns current minute's usage statistics
- `get_rpm_limit()`: Gets the RPM limit from SSM (cached for 5 minutes)

## How It Works

1. Before each Adobe API call (autotag or extract), the ECS task calls `acquire_token()`
2. The function attempts an atomic increment on the DynamoDB counter for the current minute
3. If the counter is below the RPM limit, the increment succeeds and the API call proceeds
4. If the counter equals the RPM limit, the function waits until the next minute window
5. The SSM parameter allows changing the RPM limit without redeployment

## Configuration

### Changing the RPM Limit

Use the provided script:
```bash
./bin/set-adobe-rpm.sh
```

Or manually via AWS CLI:
```bash
aws ssm put-parameter \
    --name "/pdf-processing/adobe-api-rpm" \
    --value "250" \
    --type String \
    --overwrite
```

Note: Running ECS tasks will pick up the new value within 5 minutes (SSM cache TTL).

### Environment Variables

The ECS task receives these environment variables:
- `RATE_LIMIT_TABLE`: DynamoDB table name for rate limiting
- `ADOBE_API_RPM_PARAM`: SSM parameter name for RPM limit

## Monitoring

### CloudWatch Dashboard Widgets

1. **Adobe API Rate Limiting**: Shows rate limit status from ECS task logs
2. **Adobe API Rate Limit Table Activity**: Shows DynamoDB write capacity consumption

### Log Messages

The rate limiter logs:
- Token acquisition: `Rate limit token acquired for {api_type}: {count}/{limit} in minute {minute_key}`
- Rate limit reached: `Rate limit reached ({limit}/min), waiting for next minute window...`
- Wait time: `Waiting {seconds} seconds for next minute window...`

## Benefits

1. **Distributed Rate Limiting**: Works across all concurrent ECS tasks
2. **No Code Changes for Limit Updates**: Change RPM via SSM parameter
3. **Automatic Cleanup**: DynamoDB TTL removes old entries
4. **Graceful Handling**: Tasks wait for tokens instead of failing
5. **Visibility**: CloudWatch dashboard shows rate limiting activity

## Throughput Calculation

With the rate limiter:
- Each PDF chunk requires 2 API calls (autotag + extract)
- At 200 RPM: ~100 chunks/minute = 6,000 chunks/hour
- At 250 RPM: ~125 chunks/minute = 7,500 chunks/hour

The `max_concurrency` in Step Functions can now be set higher since the rate limiter handles throttling at the API call level.
