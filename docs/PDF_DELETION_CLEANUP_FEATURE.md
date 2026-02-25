# PDF Failure Cleanup Feature

## Overview

Automatically handle PDFs when processing fails in the Step Function pipeline. **PDFs are NEVER deleted** - they are moved back to the queue for automatic retry. After a configurable number of retries (default: 3), PDFs move to a `failed/` folder for manual review.

## Key Behavior

- **ALL failures** move PDFs back to `queue/` folder for automatic retry
- Retry count is tracked in S3 object metadata
- After MAX_RETRIES failures, PDFs move to `failed/` folder (not deleted)
- Temp files are always cleaned up
- Failure records stored for daily digest email
- Folder structure is preserved throughout: `pdf/collection/doc.pdf` → `queue/collection/doc.pdf`

## Configuration

### SSM Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `/pdf-processing/max-retries` | 3 | Number of retry attempts before moving to failed/ |

Set via CLI:
```bash
./bin/set-max-retries.sh 5  # Allow 5 retries before giving up
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        FAILURE CLEANUP FLOW                                     │
│                                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐    ┌────────────────────────────┐ │
│  │ Step Function    │    │ EventBridge Rule │    │ Cleanup Lambda             │ │
│  │ Execution FAILS  │───▶│ (catches FAILED, │───▶│                            │ │
│  │ or TIMES_OUT     │    │  TIMED_OUT,      │    │ 1. Parse execution input   │ │
│  │ or ABORTS        │    │  ABORTED)        │    │ 2. Get retry count         │ │
│  └──────────────────┘    └──────────────────┘    │ 3. If < MAX_RETRIES:       │ │
│                                                  │    Move to queue/          │ │
│                                                  │ 4. If >= MAX_RETRIES:      │ │
│                                                  │    Move to failed/         │ │
│                                                  │ 5. Delete temp folder      │ │
│                                                  │ 6. Store failure record    │ │
│                                                  └────────────────────────────┘ │
│                                                               │                 │
│                      ┌────────────────────────────────────────┘                 │
│                      ▼                                                          │
│              ┌──────────────┐                                                   │
│              │ DynamoDB     │                                                   │
│              │ Failure      │                                                   │
│              │ Records      │                                                   │
│              └──────────────┘                                                   │
│                      │                                                          │
└──────────────────────┼──────────────────────────────────────────────────────────┘
                       │
                       │  Daily at 11:55 PM
                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DAILY DIGEST FLOW                                        │
│                                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐    ┌────────────────────────────┐ │
│  │ EventBridge      │    │ Email Digest     │    │ For each user with         │ │
│  │ Schedule         │───▶│ Lambda           │───▶│ failures today:            │ │
│  │ (cron 55 23 * *) │    │                  │    │                            │ │
│  └──────────────────┘    │ 1. Query today's │    │ - Lookup email in          │ │
│                          │    failures      │    │   notification table       │ │
│                          │ 2. Group by user │    │ - Send digest email        │ │
│                          │ 3. Send emails   │    │ - Mark as notified         │ │
│                          │ 4. Mark notified │    │                            │ │
│                          └──────────────────┘    └────────────────────────────┘ │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## S3 Path Flow

### Normal Retry (retry_count < MAX_RETRIES)
```
FAILURE: pdf/reports-2025/quarterly-report.pdf
         │
         ▼ moves to (with incremented retry-count metadata)
QUEUE:   queue/reports-2025/quarterly-report.pdf
         │
         ▼ queue processor picks up when capacity available
PROCESS: pdf/reports-2025/quarterly-report.pdf
```

### Max Retries Exceeded (retry_count >= MAX_RETRIES)
```
FAILURE: pdf/reports-2025/quarterly-report.pdf (retry-count: 3)
         │
         ▼ moves original PDF to
FAILED:  failed/reports-2025/quarterly-report.pdf
         │
         ▼ failure analysis generates
REPORT:  reports/failure_analysis/quarterly-report_analysis_{timestamp}.docx
```

### Temp Folder Cleanup (always happens)
```
DELETE: temp/reports-2025/quarterly-report/
        (entire folder and all contents - chunks, extracted data, etc.)
```

## S3 Object Metadata

Retry count is tracked in S3 object metadata:

| Metadata Key | Description |
|--------------|-------------|
| `retry-count` | Number of times this PDF has failed processing |
| `max-retries-exceeded` | Set to "true" when moved to failed/ folder |

## Folder Structure

| Folder | Purpose |
|--------|---------|
| `queue/` | PDFs waiting to be processed (teams upload here) |
| `pdf/` | PDFs currently being processed |
| `temp/` | Temporary processing files (chunks, extracted data) |
| `result/` | Successfully processed PDFs |
| `failed/` | Original PDFs that exceeded max retries |
| `reports/failure_analysis/` | Failure analysis reports (.docx) |

## Components

### 1. Cleanup Lambda

Triggered by EventBridge when Step Function fails.

**Responsibilities:**
1. Parse the failed execution input to extract PDF path
2. Get current retry count from S3 object metadata
3. If retry_count < MAX_RETRIES: Move PDF to `queue/` folder (increment retry count)
4. If retry_count >= MAX_RETRIES: Move original PDF to `failed/` folder
5. Delete all temp files from `/temp/[folder]/[filename]/`
6. Invoke failure analysis Lambda (generates .docx report)
7. Store failure record in DynamoDB
8. Log the cleanup action to CloudWatch

### 2. Queue Processor Lambda

Runs every 2 minutes, moves files from `queue/` to `pdf/` at controlled rate.

**Priority:**
1. Process `retry/` files first (legacy, for backwards compatibility)
2. Then process `queue/` files

### 3. DynamoDB Failure Records

| Attribute | Type | Description |
|-----------|------|-------------|
| failure_id | String (PK) | UUID for the failure record |
| failure_date | String (GSI PK) | Date in YYYY-MM-DD format |
| timestamp | String | ISO timestamp of failure |
| iam_username | String | Who uploaded the PDF |
| pdf_key | String | Original S3 key of PDF |
| queue_key | String | New location in queue/ (if retrying) |
| failed_key | String | New location in failed/ (if max retries exceeded) |
| retry_count | Number | Current retry count |
| max_retries_exceeded | Boolean | Whether max retries was exceeded |
| failure_reason | String | Why the Step Function failed |
| notified | Boolean | Whether digest email was sent |

### 4. CloudWatch Log Format

```json
{
  "timestamp": "2025-02-25T14:30:00Z",
  "event_type": "PIPELINE_FAILURE_CLEANUP",
  "action": "MOVED_TO_QUEUE",
  "execution_arn": "arn:aws:states:...:execution:...",
  "failure_reason": "ECS task failed",
  "pdf_key": "pdf/reports-2025/quarterly-report.pdf",
  "queue_key": "queue/reports-2025/quarterly-report.pdf",
  "retry_count": 2,
  "uploaded_by": "jane.doe"
}
```

## Helper Scripts

| Script | Purpose |
|--------|---------|
| `bin/set-max-retries.sh [count]` | Set max retry count (default: 3) |
| `bin/check-queue-status.sh` | Check queue/retry folder status |
| `bin/clear-rate-limit-table.sh` | Clear in-flight tracking table |

## Testing

1. Upload a PDF that will fail (e.g., corrupted file)
2. Wait for Step Function to fail
3. Verify:
   - PDF moved to `queue/` folder (not deleted)
   - Retry count incremented in metadata
   - Temp folder deleted
   - CloudWatch log entry exists
   - DynamoDB record created
4. Let it fail MAX_RETRIES times
5. Verify:
   - PDF moved to `failed/` folder
   - Placeholder file created
   - Daily digest includes the failure

## Migration Notes

- The `retry/` folder is now legacy but still supported
- Queue processor handles both `retry/` and `queue/` folders
- Old behavior (deleting PDFs) is completely removed
- All failures now result in retry or move to failed/
