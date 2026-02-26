# In-Flight Counter Monitoring & Auto-Recovery

## Overview

The PDF processing system uses an in-flight counter in DynamoDB to track how many Adobe API requests are currently active. This prevents overwhelming Adobe's API with too many concurrent requests.

However, if an ECS task crashes or times out without properly releasing its slot, the counter can get "stuck" at a non-zero value, blocking all new processing even when no tasks are actually running.

## Automatic Recovery: In-Flight Reconciler

A scheduled Lambda (`in-flight-reconciler`) runs every 5 minutes to detect and fix stuck counters.

### How It Works

1. **Compares counter vs reality**: Checks the in-flight counter against:
   - Number of tracked file entries (which have 1-hour TTL)
   - Number of running ECS tasks
   - Number of running Step Function executions

2. **Resets if clearly stale**: If the counter is positive but nothing is actually running, it resets to 0.

3. **Cleans up stale entries**: Marks file tracking entries older than 2 hours as released.

4. **Publishes metrics**: Sends data to CloudWatch for monitoring.

### Configuration (SSM Parameters)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `/pdf-processing/reconciler-enabled` | `true` | Enable/disable the reconciler |
| `/pdf-processing/reconciler-max-drift` | `5` | Max allowed difference between counter and tracked files before reset |

## Manual Intervention

### Check Current Status

```bash
bin/check-in-flight-status.sh
```

This shows:
- Current in-flight counter value
- Number of tracked files
- Running Step Functions
- Global backoff status
- Files waiting in queue/
- Diagnosis of whether counter is stuck

### Manual Reset

If you need to immediately reset the counter (don't want to wait for reconciler):

```bash
bin/reset-AIFRRT-in-flight-value-to-zero.sh
```

## CloudWatch Monitoring

The dashboard includes a "In-Flight Counter Reconciliation" widget showing:
- In-flight counter value over time
- Tracked files count
- Running ECS tasks
- Counter reset events (red line indicates reconciler intervention)

## Root Causes of Stuck Counters

1. **ECS task crash**: Container exits without calling `release_slot()`
2. **Task timeout**: Step Function times out the ECS task
3. **Network issues**: DynamoDB update fails during slot release
4. **Code bugs**: Exception thrown before `finally` block executes

## Prevention Measures

The system has multiple layers of protection:

1. **TTL on file entries**: Individual file tracking entries expire after 1 hour
2. **Reconciler Lambda**: Runs every 5 minutes to detect/fix drift
3. **CloudWatch metrics**: Visibility into counter health
4. **Manual scripts**: Quick intervention when needed

## Troubleshooting

### Queue not processing files

1. Run `bin/check-in-flight-status.sh`
2. If stuck counter detected, run `bin/reset-AIFRRT-in-flight-value-to-zero.sh`
3. Check CloudWatch logs for reconciler activity

### Reconciler not fixing the issue

1. Check if reconciler is enabled: `aws ssm get-parameter --name /pdf-processing/reconciler-enabled`
2. Check Lambda logs: `/aws/lambda/in-flight-reconciler`
3. Verify Lambda has correct permissions

### Counter keeps getting stuck

If this happens frequently, investigate:
1. ECS task logs for crashes
2. Step Function execution history for timeouts
3. Consider increasing task memory/timeout
