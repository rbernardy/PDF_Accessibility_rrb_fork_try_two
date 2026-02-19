# PDF Deletion Cleanup Feature

## Overview

Automatically clean up temporary files when a PDF is deleted from the S3 bucket, with logging, dashboard visibility, and email notifications to the user who performed the deletion.

## Feature Requirements

1. Detect when a PDF is deleted from `/pdf/[folder]/filename.pdf`
2. Automatically delete the corresponding `/temp/[folder]/[filename minus extension]/` folder and all contents
3. Log all deletion actions to a dedicated CloudWatch log group
4. Add a dashboard widget showing deletion activity
5. Send email notification to the user who deleted the PDF (configurable per IAM user)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DELETION FLOW                                     │
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────────────┐ │
│  │ User deletes │    │ S3 Event     │    │ Cleanup Lambda                 │ │
│  │ PDF via      │───▶│ Notification │───▶│                                │ │
│  │ Console/     │    │ (DeleteObject│    │ 1. Parse deleted file path     │ │
│  │ Cyberduck/   │    │  on /pdf/*)  │    │ 2. Delete /temp/[folder]/[name]│ │
│  │ CLI          │    │              │    │ 3. Query CloudTrail for user   │ │
│  └──────────────┘    └──────────────┘    │ 4. Log to CloudWatch           │ │
│                                          │ 5. Lookup email in DynamoDB    │ │
│                                          │ 6. Send email via SES          │ │
│                                          └────────────────────────────────┘ │
│                                                       │                     │
│                      ┌────────────────────────────────┼──────────────┐      │
│                      ▼                                ▼              ▼      │
│              ┌──────────────┐              ┌──────────────┐  ┌────────────┐ │
│              │ CloudWatch   │              │ DynamoDB     │  │ SES Email  │ │
│              │ Log Group    │              │ User→Email   │  │            │ │
│              │ /pdf-cleanup │              │ Table        │  │            │ │
│              └──────────────┘              └──────────────┘  └────────────┘ │
│                      │                                                      │
│                      ▼                                                      │
│              ┌──────────────┐                                               │
│              │ Dashboard    │                                               │
│              │ Widget       │                                               │
│              └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## S3 Path Mapping

When a PDF is deleted:
```
DELETE: s3://bucket/pdf/reports-2025/quarterly-report.pdf
        └─────────────┬─────────────────────────────────┘
                      │
                      ▼ triggers cleanup of
DELETE: s3://bucket/temp/reports-2025/quarterly-report/
        └─────────────────────────────────────────────┘
        (entire folder and all contents)
```

## Components

### 1. S3 Event Notification

Configure S3 to trigger Lambda on `s3:ObjectRemoved:*` events for the `/pdf/` prefix.

```python
# CDK
bucket.add_event_notification(
    s3.EventType.OBJECT_REMOVED,
    s3n.LambdaDestination(cleanup_lambda),
    s3.NotificationKeyFilter(prefix="pdf/")
)
```

### 2. DynamoDB Table for Email Configuration

```python
# CDK
notification_table = dynamodb.Table(
    self, "PdfCleanupNotifications",
    table_name="pdf-cleanup-notifications",
    partition_key=dynamodb.Attribute(
        name="iam_username",
        type=dynamodb.AttributeType.STRING
    ),
    billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
    removal_policy=RemovalPolicy.RETAIN
)
```

**Table Schema:**

| Attribute | Type | Description |
|-----------|------|-------------|
| iam_username | String (PK) | IAM username (e.g., "jane.doe") |
| email | String | Email address for notifications |
| enabled | Boolean | Whether notifications are enabled |
| created_at | String | ISO timestamp when record was created |
| updated_at | String | ISO timestamp of last update |

### 3. CloudWatch Log Group

```python
# CDK
cleanup_log_group = logs.LogGroup(
    self, "PdfCleanupLogGroup",
    log_group_name="/pdf-processing/cleanup",
    retention=logs.RetentionDays.THREE_MONTHS,
    removal_policy=RemovalPolicy.RETAIN
)
```

**Log Format:**

```json
{
  "timestamp": "2025-02-19T14:30:00Z",
  "event_type": "PDF_DELETED",
  "deleted_pdf": "pdf/reports-2025/quarterly-report.pdf",
  "deleted_temp_folder": "temp/reports-2025/quarterly-report/",
  "temp_files_deleted": 15,
  "deleted_by": "jane.doe",
  "deleted_by_arn": "arn:aws:iam::123456789:user/jane.doe",
  "deletion_method": "console",
  "email_sent": true,
  "email_recipient": "jane.doe@company.com"
}
```

### 4. Dashboard Widget

```python
# CDK - Add to existing dashboard
dashboard.add_widgets(
    cloudwatch.LogQueryWidget(
        title="PDF Deletion Activity",
        log_group_names=["/pdf-processing/cleanup"],
        query_string="""
            fields @timestamp, deleted_pdf, deleted_by, temp_files_deleted
            | filter event_type = "PDF_DELETED"
            | sort @timestamp desc
            | limit 50
        """,
        width=12,
        height=6
    )
)
```

### 5. SES Email Configuration

Ensure SES is configured:
- Verify the sender email/domain
- If in sandbox mode, verify recipient emails or request production access

```python
# CDK
ses_identity = ses.EmailIdentity(
    self, "PdfCleanupSender",
    identity=ses.Identity.email("pdf-cleanup@yourdomain.com")
)
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

# Examples:
./bin/manage-notifications.py add jane.doe jane.doe@company.com
./bin/manage-notifications.py remove john.smith
./bin/manage-notifications.py list
```

## Lambda Function: pdf-cleanup

Located at `/lambda/pdf-cleanup/`

**Responsibilities:**

1. Receive S3 delete event
2. Parse the deleted PDF path
3. Construct the corresponding temp folder path
4. List and delete all objects in the temp folder
5. Query CloudTrail to identify who deleted the PDF
6. Log the action to CloudWatch
7. Look up the user's email in DynamoDB
8. Send email notification via SES

## Email Template

**Subject:** `PDF Cleanup Complete: [filename]`

**Body:**
```
PDF Deletion Summary
====================

The following PDF and its associated temporary files have been deleted:

PDF File: s3://bucket/pdf/reports-2025/quarterly-report.pdf
Deleted At: 2025-02-19 14:30:00 UTC
Deleted By: jane.doe

Temporary Files Cleaned Up:
- Folder: s3://bucket/temp/reports-2025/quarterly-report/
- Files Deleted: 15

This is an automated notification. No action is required.
```

## IAM Permissions Required

The cleanup Lambda needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket",
        "arn:aws:s3:::your-bucket/temp/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudtrail:LookupEvents"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/pdf-cleanup-notifications"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ses:SendEmail"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/pdf-processing/cleanup:*"
    }
  ]
}
```

## CloudTrail User Identification

CloudTrail captures the identity for S3 delete operations:

```python
def get_deletion_user(bucket: str, key: str, event_time: datetime) -> dict:
    """
    Query CloudTrail to find who deleted the S3 object.
    """
    cloudtrail = boto3.client('cloudtrail')
    
    response = cloudtrail.lookup_events(
        LookupAttributes=[
            {'AttributeKey': 'EventName', 'AttributeValue': 'DeleteObject'},
            {'AttributeKey': 'ResourceName', 'AttributeValue': f'{bucket}/{key}'}
        ],
        StartTime=event_time - timedelta(minutes=5),
        EndTime=event_time + timedelta(minutes=5),
        MaxResults=1
    )
    
    if response['Events']:
        event = json.loads(response['Events'][0]['CloudTrailEvent'])
        user_identity = event['userIdentity']
        
        # Extract username from ARN
        arn = user_identity.get('arn', '')
        username = arn.split('/')[-1] if '/' in arn else user_identity.get('userName', 'unknown')
        
        return {
            'username': username,
            'arn': arn,
            'type': user_identity.get('type', 'unknown')  # IAMUser, AssumedRole, etc.
        }
    
    return {'username': 'unknown', 'arn': '', 'type': 'unknown'}
```

## Files Created

| File | Purpose |
|------|---------|
| `docs/PDF_DELETION_CLEANUP_FEATURE.md` | This design document |
| `bin/manage-notifications.py` | CLI tool to manage user email notifications |
| `lambda/pdf-cleanup/main.py` | Lambda function for cleanup logic |
| `lambda/pdf-cleanup/requirements.txt` | Python dependencies |

## CDK Changes Required

Add to `cdk/cdk_stack.py`:
- DynamoDB table for notifications
- CloudWatch log group
- Lambda function with S3 trigger
- Dashboard widget
- IAM permissions

## Testing

1. Add a test user: `./bin/manage-notifications.py add testuser test@example.com`
2. Upload a PDF to `/pdf/test-folder/test.pdf`
3. Wait for processing to create `/temp/test-folder/test/` files
4. Delete the PDF from S3
5. Verify:
   - Temp folder is deleted
   - CloudWatch log entry exists
   - Email is received
   - Dashboard shows the deletion
