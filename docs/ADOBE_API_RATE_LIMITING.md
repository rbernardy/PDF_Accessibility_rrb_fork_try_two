# Adobe API Rate Limiting Implementation

## Overview

This document describes the distributed in-flight tracking implementation for Adobe PDF Services API calls. The rate limiter ensures that all concurrent ECS tasks stay within the configured maximum concurrent requests, preventing 429 "Too Many Requests" errors from Adobe.

## The Problem with Per-Minute Counting

The previous implementation used per-minute counters, which had a fundamental flaw:

```
Time 0:00 - Task A acquires token (count: 1/200), starts API call
Time 0:00 - Task B acquires token (count: 2/200), starts API call
...
Time 0:00 - Task 100 acquires token (count: 100/200), starts API call
Time 0:05 - All 100 API calls hit Adobe simultaneously
Time 0:05 - Adobe sees burst of 100 requests, returns 429 for some
```

The issue: counting when requests **start** doesn't account for requests still **in progress**.

## The Solution: In-Flight Tracking

The new implementation tracks requests that are currently "in flight":

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ECS Task (Adobe Autotag)                          │
│                                                                             │
│   1. acquire_slot() ──────────────────────────────────────────────────┐     │
│      - Increment in_flight counter                                    │     │
│      - Wait if in_flight >= max_in_flight                            │     │
│                                                                       │     │
│   2. Make Adobe API call ◄────────────────────────────────────────────┤     │
│                                                                       │     │
│   3. release_slot() (in finally block) ──────────────────────────────┘     │
│      - Decrement in_flight counter                                          │
│      - Always runs, even on failure                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Key differences:
- **Old**: Counts starts per minute (no completion tracking)
- **New**: Tracks in-flight count (increment on start, decrement on finish)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DynamoDB Table                                    │
│                                                                             │
│   { "counter_id": "adobe_api_in_flight",                                   │
│     "in_flight": 45,                                                        │
│     "last_updated": "2026-02-22T10:30:00Z" }                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                    ▲                           │
                    │ decrement                 │ increment
                    │ (release_slot)            │ (acquire_slot)
                    │                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ECS Tasks (up to 100 concurrent)                    │
│                                                                             │
│   Task 1: [waiting] ──► [in-flight] ──► [complete]                         │
│   Task 2: [waiting] ──► [in-flight] ──► [complete]                         │
│   Task 3: [in-flight] ──► [complete]                                        │
│   ...                                                                       │
│   Task N: [waiting for slot]                                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. DynamoDB In-Flight Counter Table

- **Table Name**: `adobe-api-in-flight-tracker`
- **Partition Key**: `counter_id` (string)
- **Attributes**:
  - `in_flight`: Current number of API calls in progress
  - `last_updated`: Timestamp of last update

### 2. SSM Parameters

- **`/pdf-processing/adobe-api-max-in-flight`**: Maximum concurrent requests (default: 150)
- **`/pdf-processing/adobe-api-rpm`**: Adobe's actual limit for reference (200)

### 3. Rate Limiter Module

Located at: `adobe-autotag-container/rate_limiter.py`

Key functions:
- `acquire_slot(api_type)`: Increment counter, wait if at capacity
- `release_slot(api_type)`: Decrement counter (must always be called)
- `get_current_in_flight()`: Get current count for monitoring
- `get_current_usage()`: Get usage stats (in_flight, max, available, utilization_pct)

## How It Works

1. **Before API call**: Task calls `acquire_slot()`
   - Attempts atomic increment with condition `in_flight < max_in_flight`
   - If at capacity, waits with exponential backoff and retries
   - Returns `True` when slot acquired

2. **API call executes**: Task makes Adobe API call

3. **After API call**: Task calls `release_slot()` in `finally` block
   - Decrements counter atomically
   - Always runs, even if API call failed
   - Frees slot for other waiting tasks

## Configuration

### Changing the Max In-Flight Limit

Via AWS CLI:
```bash
aws ssm put-parameter \
    --name "/pdf-processing/adobe-api-max-in-flight" \
    --value "175" \
    --type String \
    --overwrite
```

Note: Running ECS tasks will pick up the new value within 5 minutes (SSM cache TTL).

### Recommended Settings

| Scenario | max_in_flight | Reasoning |
|----------|---------------|-----------|
| Conservative | 100 | Safe margin, slower throughput |
| Balanced | 150 | Good balance (default) |
| Aggressive | 180 | Higher throughput, closer to limit |

The default of 150 provides a 25% buffer below Adobe's 200 RPM limit, accounting for:
- Timing variations in request completion
- Burst protection
- Network latency variations

### Environment Variables

The ECS task receives:
- `RATE_LIMIT_TABLE`: DynamoDB table name
- `ADOBE_API_MAX_IN_FLIGHT_PARAM`: SSM parameter for max concurrent requests
- `ADOBE_API_RPM_PARAM`: SSM parameter for Adobe's RPM limit (reference)

## Monitoring

### CloudWatch Dashboard Widgets

1. **Adobe API In-Flight Requests**: Real-time custom widget showing:
   - Current in-flight count
   - Max allowed
   - Available slots
   - Utilization percentage

2. **In-Flight Tracking Logs**: Shows slot acquisition and release events

3. **DynamoDB Activity**: Write capacity consumption

### Log Messages

The rate limiter logs:
- Slot acquired: `[autotag] Acquired slot: 45/150 in-flight`
- At capacity: `[autotag] At capacity (150/150), waiting 2.0s (attempt 1)...`
- Slot released: `[autotag] Released slot: 44 now in-flight`

## Benefits

1. **True Rate Limiting**: Tracks actual in-progress requests, not just starts
2. **High Concurrency Safe**: Can run 100+ ECS tasks without API errors
3. **Self-Healing**: Slots always released via `finally` blocks
4. **Configurable**: Change limits via SSM without redeployment
5. **Observable**: Real-time dashboard shows queue status

## Throughput Calculation

With in-flight tracking at 150 max:
- Each PDF chunk requires 2 API calls (autotag + extract)
- Average API call duration: ~10-15 seconds
- Effective throughput: ~10-15 API calls completing per second
- With 150 in-flight: ~600-900 API calls per minute (well under 200 RPM sustained)

The `max_concurrency=100` in Step Functions allows fast parallel processing of non-API work, while the in-flight tracker gates actual API calls.

## Failure Handling

If an ECS task crashes without releasing its slot:
- The in-flight counter may become slightly inflated
- This is self-correcting: as other tasks complete, slots free up
- Worst case: temporary reduced throughput until counter normalizes

For persistent issues, manually reset the counter:
```bash
aws dynamodb put-item \
    --table-name adobe-api-in-flight-tracker \
    --item '{"counter_id": {"S": "adobe_api_in_flight"}, "in_flight": {"N": "0"}}'
```
