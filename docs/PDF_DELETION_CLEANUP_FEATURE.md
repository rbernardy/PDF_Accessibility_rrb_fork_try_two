# PDF Failure Cleanup Feature

## Overview

Automatically clean up PDFs and their temporary files when processing fails in the Step Function pipeline. Failures are logged and users receive a daily digest email summarizing what failed and was cleaned up.

## Feature Requirements

1. Detect when a PDF fails processing (Step Function execution fails/times out/aborts)
2. Automatically delete the original PDF from `/pdf/[folder]/filename.pdf`
3. Automatically delete the temp folder `/temp/[folder]/[filename minus extension]/` and all contents
4. Log all cleanup actions to a dedicated CloudWatch log group
5. Store failure records in DynamoDB for daily digest
6. Send ONE daily digest email per user at 11:55 PM with all their failures for that day
7. Add dashboard widgets showing failure/cleanup activity

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        FAILURE CLEANUP FLOW                                     │
│                                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐    ┌────────────────────────────┐ │
│  │ Step Function    │    │ EventBridge Rule │    │ Cleanup Lambda             │ │
│  │ Execution FAILS  │───▶│ (catches FAILED, │───▶│                            │ │
│  │ or TIMES_OUT     │    │  TIMED_OUT,      │    │ 1. Parse execution input   │ │
│  │ or ABORTS        │    │  ABORTED)        │    │ 2. Delete /pdf/[file].pdf  │ │
│  └──────────────────┘    └──────────────────┘    │ 3. Delete /temp/[folder]/  │ │
│                                                  │ 4. Query CloudTrail for    │ │
│                                                  │    uploader                │ │
│                                                  │ 5. Store in DynamoDB       │ │
│                                                  │ 6. Log to CloudWatch       │ │
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

## S3 Path Mapping

When a PDF fails processing:
```
DELETE: s3://bucket/pdf/reports-2025/quarterly-report.pdf
        └─────────────────────────────────────────────────┘
                      │
                      ▼ also deletes
DELETE: s3://bucket/temp/reports-2025/quarterly-report/
        └─────────────────────────────────────────────────┘
        (entire folder and all contents)
```

## Components

### 1. EventBridge Rule for Step Function Failures

Triggers on Step Function execution state changes to FAILED, TIMED_OUT, or ABORTED.

```python
# CDK
failure_rule = events.Rule(
    self, "PdfProcessingFailureRule",
    event_pattern=events.EventPattern(
        source=["aws.states"],
        detail_type=["Step Functions Execution Status Change"],
        detail={
            "stateMachineArn": [state_machine.state_machine_arn],
            "status": ["FAILED", "TIMED_OUT", "ABORTED"]
        }
    )
)
failure_rule.add_target(targets.LambdaFunction(cleanup_lambda))
```

### 2. DynamoDB Tables

#### Failure Records Table
Stores each failure for daily digest processing.

| Attribute | Type | Description |
|-----------|------|-------------|
| failure_id | String (PK) | UUID for the failure record |
| failure_date | String (GSI PK) | Date in YYYY-MM-DD format for querying |
| timestamp | String | ISO timestamp of failure |
| iam_username | String | Who uploaded the PDF |
| user_arn | String | Full ARN of uploader |
| pdf_key | String | S3 key of deleted PDF |
| temp_folder | String | S3 prefix of deleted temp folder |
| temp_files_deleted | Number | Count of temp files deleted |
| failure_reason | String | Why the Step Function failed |
| execution_arn | String | Step Function execution ARN |
| notified | Boolean | Whether digest email was sent |

#### Notification Preferences Table
Maps IAM usernames to email addresses (managed via CLI tool).

| Attribute | Type | Description |
|-----------|------|-------------|
| iam_username | String (PK) | IAM username |
| email | String | Email address for notifications |
| enabled | Boolean | Whether notifications are enabled |

### 3. Cleanup Lambda

Triggered by EventBridge when Step Function fails.

**Responsibilities:**
1. Parse the failed execution input to extract PDF path
2. Delete the original PDF from `/pdf/`
3. Delete all temp files from `/temp/[folder]/[filename]/`
4. Query CloudTrail to find who uploaded the PDF (PutObject event)
5. Store failure record in DynamoDB
6. Log the cleanup action to CloudWatch

### 4. Email Digest Lambda

Triggered daily at 11:55 PM by EventBridge schedule.

**Responsibilities:**
1. Query DynamoDB for all failures from today where `notified = false`
2. Group failures by `iam_username`
3. For each user, look up their email in the notification preferences table
4. Send one digest email per user summarizing all their failures
5. Mark all processed records as `notified = true`

### 5. CloudWatch Log Group

Dedicated log group `/pdf-processing/cleanup` for all cleanup events.

**Log Format:**
```json
{
  "timestamp": "2025-02-19T14:30:00Z",
  "event_type": "PIPELINE_FAILURE_CLEANUP",
  "execution_arn": "arn:aws:states:...:execution:...",
  "failure_reason": "ECS task failed",
  "deleted_pdf": "pdf/reports-2025/quarterly-report.pdf",
  "deleted_temp_folder": "temp/reports-2025/quarterly-report/",
  "temp_files_deleted": 15,
  "uploaded_by": "jane.doe",
  "uploaded_by_arn": "arn:aws:iam::123456789:user/jane.doe"
}
```

### 6. Dashboard Widgets

- Pipeline Failure Cleanup Activity
- Failures by User (today)
- Daily Digest Emails Sent

## Email Digest Template

**Subject:** `PDF Processing Failures - Daily Summary for [date]`

**Body:**
```
PDF Processing Failure Summary
==============================

Date: February 19, 2025
User: jane.doe

The following PDFs failed processing and have been automatically cleaned up:

1. quarterly-report.pdf
   - Original location: pdf/reports-2025/quarterly-report.pdf
   - Failure reason: ECS task timeout
   - Temp files deleted: 15
   - Failed at: 2025-02-19 10:30:00 UTC

2. annual-summary.pdf
   - Original location: pdf/reports-2025/annual-summary.pdf
   - Failure reason: Adobe API error
   - Temp files deleted: 8
   - Failed at: 2025-02-19 14:45:00 UTC

Total failures today: 2

To retry processing, please re-upload the original PDF files to the appropriate folder.

This is an automated notification.
```

## CLI Tool: manage-notifications.py

Located at `/bin/manage-notifications.py`

**Usage:**
```bash
# Add or update a user's notification email
./bin/manage-notifications.py add <iam_username> <email>

# Remove a user from notifications
./bin/manage-notifications.py remove <iam_username>

# Disable notifications for a user (keeps record)
./bin/manage-notifications.py disable <iam_username>

# Enable notifications for a user
./bin/manage-notifications.py enable <iam_username>

# List all configured users
./bin/manage-notifications.py list
```

## IAM Permissions Required

### Cleanup Lambda
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::bucket", "arn:aws:s3:::bucket/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["cloudtrail:LookupEvents"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:*:*:table/pdf-failure-records"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:log-group:/pdf-processing/cleanup:*"
    }
  ]
}
```

### Email Digest Lambda
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:Query", "dynamodb:UpdateItem"],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/pdf-failure-records",
        "arn:aws:dynamodb:*:*:table/pdf-failure-records/index/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem"],
      "Resource": "arn:aws:dynamodb:*:*:table/pdf-cleanup-notifications"
    },
    {
      "Effect": "Allow",
      "Action": ["ses:SendEmail"],
      "Resource": "*"
    }
  ]
}
```

## Files

| File | Purpose |
|------|---------|
| `docs/PDF_DELETION_CLEANUP_FEATURE.md` | This design document |
| `bin/manage-notifications.py` | CLI tool to manage user email notifications |
| `lambda/pdf-failure-cleanup/main.py` | Lambda for cleanup on failure |
| `lambda/pdf-failure-cleanup/requirements.txt` | Dependencies |
| `lambda/pdf-failure-cleanup/Dockerfile` | Container build |
| `lambda/pdf-failure-digest/main.py` | Lambda for daily email digest |
| `lambda/pdf-failure-digest/requirements.txt` | Dependencies |
| `lambda/pdf-failure-digest/Dockerfile` | Container build |

## Testing

1. Add a test user: `./bin/manage-notifications.py add testuser test@example.com`
2. Upload a PDF that will fail (e.g., corrupted file)
3. Wait for Step Function to fail
4. Verify:
   - PDF is deleted from `/pdf/`
   - Temp folder is deleted
   - CloudWatch log entry exists
   - DynamoDB record created
5. Wait until 11:55 PM (or manually invoke digest Lambda)
6. Verify email received with failure summary
