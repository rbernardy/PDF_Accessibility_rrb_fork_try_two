# Adobe API Queue Architecture

## Overview

This document outlines a two-queue architecture to handle Adobe PDF Services API calls (AutoTag and Extract) with built-in rate limiting, retry logic, and failure isolation.

## Problem Statement

- Each PDF chunk requires 2 Adobe API calls: AutoTag → Extract
- Adobe rate limit: 200 requests/minute
- ~250,000 PDFs to process (potentially 1M+ chunks)
- Current synchronous ECS tasks can overwhelm the rate limit and fail entirely on transient errors

## Architecture

```
                                    ┌─────────────────────┐
                                    │   S3 Trigger /      │
                                    │   Step Function     │
                                    └──────────┬──────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              AUTOTAG STAGE                                   │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐   │
│  │  Autotag Queue  │───▶│  Autotag Worker │───▶│  Adobe AutoTag API      │   │
│  │  (SQS)          │    │  (Lambda/ECS)   │    │                         │   │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘   │
│          │                      │                                            │
│          ▼                      │ on success                                 │
│  ┌─────────────────┐            │                                            │
│  │  Autotag DLQ    │            │                                            │
│  └─────────────────┘            │                                            │
└─────────────────────────────────┼────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                              EXTRACT STAGE                                   │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐   │
│  │  Extract Queue  │───▶│  Extract Worker │───▶│  Adobe Extract API      │   │
│  │  (SQS)          │    │  (Lambda/ECS)   │    │                         │   │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘   │
│          │                      │                                            │
│          ▼                      │ on success                                 │
│  ┌─────────────────┐            │                                            │
│  │  Extract DLQ    │            │                                            │
│  └─────────────────┘            │                                            │
└─────────────────────────────────┼────────────────────────────────────────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  Post-Processing│
                         │  (TOC, images,  │
                         │   S3 upload)    │
                         └─────────────────┘
```

## Queue Message Schemas

### Autotag Queue Message

```json
{
  "bucket": "your-bucket-name",
  "chunk_key": "uploads/job-123/chunks/chunk_001.pdf",
  "original_file_key": "uploads/job-123/original.pdf",
  "job_id": "job-123",
  "timestamp": "2025-02-19T10:30:00Z"
}
```

### Extract Queue Message

```json
{
  "bucket": "your-bucket-name",
  "chunk_key": "uploads/job-123/chunks/chunk_001.pdf",
  "autotagged_key": "uploads/job-123/output_autotag/COMPLIANT_chunk_001.pdf",
  "original_file_key": "uploads/job-123/original.pdf",
  "job_id": "job-123",
  "timestamp": "2025-02-19T10:30:05Z"
}
```

## Component Details

### 1. Autotag Queue (SQS)

```python
# CDK Definition
autotag_queue = sqs.Queue(
    self, "AdobeAutotagQueue",
    queue_name="adobe-autotag-queue",
    visibility_timeout=Duration.minutes(15),  # Must exceed worker timeout
    retention_period=Duration.days(7),
    dead_letter_queue=sqs.DeadLetterQueue(
        max_receive_count=3,  # Retry 3 times before DLQ
        queue=autotag_dlq
    )
)
```

### 2. Autotag Worker (Lambda or ECS)

```python
# autotag_worker.py

import json
import boto3
from adobe.pdfservices.operation.exception.exceptions import ServiceUsageException

sqs = boto3.client('sqs')
EXTRACT_QUEUE_URL = os.environ['EXTRACT_QUEUE_URL']

def handler(event, context):
    for record in event['Records']:
        message = json.loads(record['body'])
        
        try:
            # Download chunk from S3
            chunk_path = download_from_s3(message['bucket'], message['chunk_key'])
            
            # Get Adobe credentials
            client_id, client_secret = get_secret()
            
            # Run AutoTag API
            autotagged_path = autotag_pdf(chunk_path, client_id, client_secret)
            
            # Upload autotagged PDF to S3
            autotagged_key = upload_to_s3(autotagged_path, message['bucket'])
            
            # Send to Extract queue
            sqs.send_message(
                QueueUrl=EXTRACT_QUEUE_URL,
                MessageBody=json.dumps({
                    'bucket': message['bucket'],
                    'chunk_key': message['chunk_key'],
                    'autotagged_key': autotagged_key,
                    'original_file_key': message['original_file_key'],
                    'job_id': message['job_id'],
                    'timestamp': datetime.utcnow().isoformat()
                })
            )
            
        except ServiceUsageException as e:
            if 'rate limit' in str(e).lower() or '429' in str(e):
                # Don't catch - let it return to queue for retry
                raise
            else:
                # Other Adobe errors - log and let DLQ handle
                raise
```

### 3. Extract Queue (SQS)

```python
# CDK Definition
extract_queue = sqs.Queue(
    self, "AdobeExtractQueue",
    queue_name="adobe-extract-queue",
    visibility_timeout=Duration.minutes(15),
    retention_period=Duration.days(7),
    dead_letter_queue=sqs.DeadLetterQueue(
        max_receive_count=3,
        queue=extract_dlq
    )
)
```

### 4. Extract Worker (Lambda or ECS)

```python
# extract_worker.py

import json
import boto3
from adobe.pdfservices.operation.exception.exceptions import ServiceUsageException

def handler(event, context):
    for record in event['Records']:
        message = json.loads(record['body'])
        
        try:
            # Download autotagged PDF from S3
            pdf_path = download_from_s3(message['bucket'], message['autotagged_key'])
            
            # Get Adobe credentials
            client_id, client_secret = get_secret()
            
            # Run Extract API
            extract_result = extract_pdf(pdf_path, client_id, client_secret)
            
            # Post-processing: TOC, images, metadata
            process_extracted_content(extract_result, message)
            
            # Upload final results to S3
            upload_final_results(message['bucket'], message['job_id'])
            
        except ServiceUsageException as e:
            if 'rate limit' in str(e).lower() or '429' in str(e):
                raise  # Return to queue for retry
            else:
                raise  # DLQ will catch persistent failures
```

## Rate Limiting Strategy

### Option A: Lambda Concurrency Limits (Simple)

```python
# CDK - Limit concurrent executions to stay under 200 req/min
autotag_worker = lambda_.Function(
    self, "AutotagWorker",
    reserved_concurrent_executions=50,  # Max 50 concurrent
    # ... other config
)

extract_worker = lambda_.Function(
    self, "ExtractWorker", 
    reserved_concurrent_executions=50,  # Max 50 concurrent
    # ... other config
)
```

With 50 concurrent workers per stage and ~10 second API calls, you get:
- ~300 requests/minute per stage
- Combined: stay comfortably under 200/min with some buffer

### Option B: Token Bucket with DynamoDB (Precise)

For precise rate limiting across distributed workers:

```python
# rate_limiter.py

import boto3
import time

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('AdobeRateLimiter')

def acquire_token(api_type: str, max_per_minute: int = 200) -> bool:
    """
    Attempt to acquire a rate limit token.
    Returns True if allowed, False if should wait.
    """
    current_minute = int(time.time() / 60)
    
    try:
        response = table.update_item(
            Key={'api_type': api_type, 'minute': current_minute},
            UpdateExpression='SET request_count = if_not_exists(request_count, :zero) + :inc',
            ConditionExpression='attribute_not_exists(request_count) OR request_count < :max',
            ExpressionAttributeValues={
                ':zero': 0,
                ':inc': 1,
                ':max': max_per_minute
            },
            ReturnValues='UPDATED_NEW'
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False  # Rate limit exceeded

def wait_for_token(api_type: str, max_per_minute: int = 200):
    """Block until a token is available."""
    while not acquire_token(api_type, max_per_minute):
        time.sleep(1)
```

## Monitoring & Observability

### CloudWatch Metrics to Track

```python
# Custom metrics in workers
cloudwatch = boto3.client('cloudwatch')

def put_metric(metric_name, value, unit='Count'):
    cloudwatch.put_metric_data(
        Namespace='AdobePDFProcessing',
        MetricData=[{
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit
        }]
    )

# Usage in workers:
put_metric('AutotagSuccess', 1)
put_metric('AutotagRateLimited', 1)
put_metric('ExtractSuccess', 1)
put_metric('ExtractRateLimited', 1)
```

### CloudWatch Dashboard Widgets

- Autotag Queue Depth (ApproximateNumberOfMessages)
- Extract Queue Depth (ApproximateNumberOfMessages)
- Autotag DLQ Depth (failures needing attention)
- Extract DLQ Depth
- API calls per minute (custom metric)
- Success/failure rates

### Alarms

```python
# CDK Alarms
cloudwatch.Alarm(
    self, "AutotagDLQAlarm",
    metric=autotag_dlq.metric_approximate_number_of_messages_visible(),
    threshold=10,
    evaluation_periods=1,
    alarm_description="Autotag failures accumulating in DLQ"
)
```

## Migration Path from Current Architecture

### Phase 1: Add Queues (Non-Breaking)

1. Create SQS queues and DLQs via CDK
2. Deploy new worker Lambdas alongside existing ECS tasks
3. Test with a small batch of PDFs

### Phase 2: Switch Traffic

1. Modify the trigger (S3 event or Step Function) to send to Autotag Queue instead of launching ECS directly
2. Monitor queue depths and processing rates
3. Keep old ECS task code as fallback

### Phase 3: Cleanup

1. Remove old synchronous ECS task triggers
2. Consolidate worker code
3. Tune concurrency limits based on observed throughput

## Estimated Throughput

With 200 requests/minute limit:

| Metric | Value |
|--------|-------|
| Effective chunks/minute | ~100 (2 API calls each) |
| Chunks/hour | ~6,000 |
| Chunks/day | ~144,000 |
| Time for 1M chunks | ~7 days |

## Files to Create/Modify

1. `cdk/cdk_stack.py` - Add SQS queues, DLQs, Lambda workers
2. `lambda/autotag-worker/main.py` - New autotag queue consumer
3. `lambda/extract-worker/main.py` - New extract queue consumer
4. `lambda/autotag-worker/requirements.txt` - Dependencies
5. `lambda/extract-worker/requirements.txt` - Dependencies

## Open Questions

1. Should workers be Lambda (simpler, auto-scaling) or ECS (longer timeout, more memory)?
2. Do we need the DynamoDB rate limiter, or is Lambda concurrency sufficient?
3. How should we handle the post-processing step (TOC, images) — same worker or third queue?
