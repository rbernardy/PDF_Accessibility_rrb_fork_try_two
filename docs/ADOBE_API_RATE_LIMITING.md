# Adobe API Rate Limiting Implementation

## Overview

This document describes the distributed rate limiting implementation for Adobe PDF Services API calls. The rate limiter uses a dual-protection system:

1. **In-Flight Tracking**: Limits concurrent API requests (default: 150)
2. **RPM Limiting**: Limits requests per minute (default: 190, under Adobe's 200 hard limit)

This ensures that all concurrent ECS tasks stay within both limits, preventing 429 "Too Many Requests" errors from Adobe.

## Why Dual Protection?

### Problem 1: Concurrent Overload (solved by In-Flight Tracking)

Without in-flight tracking, many ECS tasks could start API calls simultaneously:

```
Time 0:00 - 100 tasks all start API calls at once
Time 0:00 - Adobe sees burst of 100 requests, returns 429 for some
```

### Problem 2: Fast-Processing PDFs (solved by RPM Limiting)

In-flight tracking alone doesn't prevent RPM violations with fast PDFs:

```
With 150 concurrent slots and 5-second processing time:
150 slots × 12 turnovers/minute = 1,800 requests/minute (way over 200!)
```

The RPM limiter ensures we never exceed 190 requests in any 60-second window.

## The Solution: Dual-Protection System

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ECS Task (Adobe Autotag)                          │
│                                                                             │
│   1. acquire_slot() ──────────────────────────────────────────────────┐     │
│      a. Check RPM limit (wait if >= 190 this minute)                 │     │
│      b. Increment in_flight counter (wait if >= 150)                 │     │
│      c. Increment RPM counter for current minute window              │     │
│                                                                       │     │
│   2. Make Adobe API call ◄────────────────────────────────────────────┤     │
│                                                                       │     │
│   3. release_slot() (in finally block) ──────────────────────────────┘     │
│      - Decrement in_flight counter                                          │
│      - Always runs, even on failure                                         │
│      - (RPM counter auto-expires, no decrement needed)                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DynamoDB Table                                    │
│                                                                             │
│   In-Flight Counter:                                                        │
│   { "counter_id": "adobe_api_in_flight",                                   │
│     "in_flight": 45,                                                        │
│     "last_updated": "2026-02-22T10:30:00Z" }                               │
│                                                                             │
│   RPM Window Counters (auto-expire after 2 minutes):                        │
│   { "counter_id": "rpm_window_20260222_1030",                              │
│     "request_count": 87,                                                    │
│     "ttl": 1708599120 }                                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                    ▲                           │
                    │ decrement in_flight       │ increment in_flight + RPM
                    │ (release_slot)            │ (acquire_slot)
                    │                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ECS Tasks (up to 100 concurrent)                    │
│                                                                             │
│   Task 1: [waiting for RPM] ──► [in-flight] ──► [complete]                 │
│   Task 2: [waiting for slot] ──► [in-flight] ──► [complete]                │
│   Task 3: [in-flight] ──► [complete]                                        │
│   ...                                                                       │
│   Task N: [waiting for next minute window]                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. DynamoDB Table

- **Table Name**: `adobe-api-in-flight-tracker`
- **Partition Key**: `counter_id` (string)
- **TTL Attribute**: `ttl` (for auto-expiring RPM window counters)

Records:
- **In-Flight Counter**: `counter_id = "adobe_api_in_flight"`, tracks concurrent requests
- **RPM Window Counters**: `counter_id = "rpm_window_YYYYMMDD_HHMM"`, tracks requests per minute

### 2. SSM Parameters

- **`/pdf-processing/adobe-api-max-in-flight`**: Maximum concurrent requests (default: 150)
- **`/pdf-processing/adobe-api-rpm`**: Maximum requests per minute (default: 190)

### 3. Rate Limiter Module

Located at: `adobe-autotag-container/rate_limiter.py`

Key functions:
- `acquire_slot(api_type)`: Check RPM limit, increment in-flight counter, increment RPM counter
- `release_slot(api_type)`: Decrement in-flight counter (must always be called)
- `get_current_in_flight()`: Get current concurrent count
- `get_current_usage()`: Get full usage stats (in_flight, RPM, utilization percentages)

## How It Works

1. **Before API call**: Task calls `acquire_slot()`
   - First checks RPM limit for current minute window
   - If at RPM limit, waits until next minute window
   - Then attempts atomic increment of in-flight counter with condition `in_flight < max_in_flight`
   - If at in-flight capacity, waits with exponential backoff and retries
   - Once both checks pass, increments RPM counter for current window
   - Returns `True` when slot acquired

2. **API call executes**: Task makes Adobe API call

3. **After API call**: Task calls `release_slot()` in `finally` block
   - Decrements in-flight counter atomically
   - Always runs, even if API call failed
   - Frees slot for other waiting tasks
   - (RPM counter auto-expires via TTL, no decrement needed)

## Configuration

### Changing the Limits

Via AWS CLI:
```bash
# Change max concurrent in-flight requests
aws ssm put-parameter \
    --name "/pdf-processing/adobe-api-max-in-flight" \
    --value "175" \
    --type String \
    --overwrite

# Change max requests per minute
aws ssm put-parameter \
    --name "/pdf-processing/adobe-api-rpm" \
    --value "180" \
    --type String \
    --overwrite
```

Note: Running ECS tasks will pick up new values within 5 minutes (SSM cache TTL).

### Recommended Settings

| Scenario | max_in_flight | max_rpm | Reasoning |
|----------|---------------|---------|-----------|
| Conservative | 100 | 150 | Maximum safety margin |
| Balanced | 150 | 190 | Good balance (default) |
| Aggressive | 180 | 195 | Higher throughput, minimal buffer |

The defaults provide:
- 150 concurrent (25% buffer for burst protection)
- 190 RPM (10 request buffer under Adobe's 200 hard limit)

### Environment Variables

The ECS task receives:
- `RATE_LIMIT_TABLE`: DynamoDB table name
- `ADOBE_API_MAX_IN_FLIGHT_PARAM`: SSM parameter for max concurrent requests
- `ADOBE_API_RPM_PARAM`: SSM parameter for max requests per minute

## Monitoring

### CloudWatch Dashboard Widgets

1. **Adobe API Rate Limiting**: Real-time custom widget showing:
   - Current in-flight count and utilization
   - Current RPM count and utilization
   - Available slots for both limits
   - Current minute window and time remaining

2. **In-Flight Files**: Shows which files are currently being processed

3. **DynamoDB Activity**: Write capacity consumption

### Log Messages

The rate limiter logs:
- Slot acquired: `[autotag] Acquired slot: 45/150 in-flight, 87/190 RPM`
- At in-flight capacity: `[autotag] At in-flight capacity (150/150), waiting 2.0s (attempt 1)...`
- At RPM limit: `[autotag] RPM limit reached (190/190), waiting 15.0s for next minute window (attempt 1)...`
- Slot released: `[autotag] Released slot: 44 now in-flight`

## Benefits

1. **Guaranteed RPM Protection**: Never exceeds 190 requests/minute regardless of PDF processing speed
2. **Concurrent Overload Protection**: Never exceeds 150 simultaneous API calls
3. **Fast PDF Safe**: Even 1000 small, fast-processing PDFs won't cause 429 errors
4. **Self-Healing**: In-flight slots always released via `finally` blocks; RPM counters auto-expire
5. **Configurable**: Change both limits via SSM without redeployment
6. **Observable**: Real-time dashboard shows both limits and utilization

## Throughput Calculation

With dual protection (150 in-flight, 190 RPM):

**Scenario 1: Large PDFs (30+ second processing)**
- Bottleneck: In-flight limit
- Throughput: ~5 completions/second = ~300/minute
- RPM limit rarely hit

**Scenario 2: Small PDFs (5 second processing)**
- Bottleneck: RPM limit
- Throughput: Capped at 190/minute
- Tasks queue waiting for next minute window

**Scenario 3: Mixed workload**
- Both limits work together
- System automatically adapts to whichever limit is constraining

The `max_concurrency=100` in Step Functions allows fast parallel processing of non-API work, while the rate limiter gates actual API calls.

## Failure Handling

**In-Flight Counter Issues:**
If an ECS task crashes without releasing its slot:
- The in-flight counter may become slightly inflated
- This is self-correcting: as other tasks complete, slots free up
- Worst case: temporary reduced throughput until counter normalizes

**RPM Counter Issues:**
- RPM window counters auto-expire after 2 minutes via DynamoDB TTL
- No manual intervention needed
- Each minute gets a fresh counter

**Manual Reset (if needed):**
```bash
# Reset in-flight counter
aws dynamodb put-item \
    --table-name adobe-api-in-flight-tracker \
    --item '{"counter_id": {"S": "adobe_api_in_flight"}, "in_flight": {"N": "0"}}'

# RPM counters auto-expire, but can be deleted manually if needed
aws dynamodb delete-item \
    --table-name adobe-api-in-flight-tracker \
    --key '{"counter_id": {"S": "rpm_window_20260222_1030"}}'
```
