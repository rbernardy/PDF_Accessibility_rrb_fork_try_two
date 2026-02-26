#!/bin/bash
#
# Check the status of the PDF processing queue
# Shows counts in queue/, retry/, pdf/, failed/, and result/ folders
#
# Usage: ./bin/check-queue-status.sh [bucket-name]
#

set -e

while true; do

clear
date +"%Y-%m-%d %H:%M:%S"
BUCKET_NAME="${1:-pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog}"

echo "=== PDF Processing Queue Status ==="
echo "Bucket: $BUCKET_NAME"
echo ""

# Check if queue processing is enabled
QUEUE_ENABLED=$(aws ssm get-parameter \
    --name "/pdf-processing/queue-enabled" \
    --query 'Parameter.Value' \
    --output text 2>/dev/null || echo "true")
if [ "$QUEUE_ENABLED" = "true" ] || [ "$QUEUE_ENABLED" = "1" ]; then
    echo "Queue Processing: ENABLED"
else
    echo "Queue Processing: PAUSED (run ./bin/queue-resume.sh to enable)"
fi
echo ""

# Count files in each folder
echo "Folder counts:"

QUEUE_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/queue/" --recursive 2>/dev/null | grep "\.pdf$" | wc -l | tr -d ' ')
QUEUE_COUNT=${QUEUE_COUNT:-0}
echo "  queue/  : $QUEUE_COUNT PDFs (waiting to be processed)"

RETRY_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/retry/" --recursive 2>/dev/null | grep "\.pdf$" | wc -l | tr -d ' ')
RETRY_COUNT=${RETRY_COUNT:-0}
echo "  retry/  : $RETRY_COUNT PDFs (legacy retry folder)"

PDF_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/pdf/" --recursive 2>/dev/null | grep "\.pdf$" | wc -l | tr -d ' ')
PDF_COUNT=${PDF_COUNT:-0}
echo "  pdf/    : $PDF_COUNT PDFs (currently processing)"

FAILED_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/failed/" --recursive 2>/dev/null | grep "\.pdf$" | wc -l | tr -d ' ')
FAILED_COUNT=${FAILED_COUNT:-0}
echo "  failed/ : $FAILED_COUNT PDFs (max retries exceeded)"

RESULT_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/result/" --recursive 2>/dev/null | grep "\.pdf$" | wc -l | tr -d ' ')
RESULT_COUNT=${RESULT_COUNT:-0}
echo "  result/ : $RESULT_COUNT PDFs (completed)"

echo ""
echo "=== Rate Limit Status ==="

# Get in-flight count from DynamoDB
IN_FLIGHT=$(aws dynamodb get-item \
    --table-name "adobe-api-in-flight-tracker" \
    --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
    --query 'Item.in_flight.N' \
    --output text 2>/dev/null || echo "0")
echo "  In-flight Adobe API calls: $IN_FLIGHT"

# Check for global backoff
BACKOFF=$(aws dynamodb get-item \
    --table-name "adobe-api-in-flight-tracker" \
    --key '{"counter_id": {"S": "global_backoff_until"}}' \
    --query 'Item.backoff_until.N' \
    --output text 2>/dev/null || echo "None")

if [ "$BACKOFF" != "None" ] && [ "$BACKOFF" != "" ]; then
    NOW=$(date +%s)
    REMAINING=$((BACKOFF - NOW))
    if [ $REMAINING -gt 0 ]; then
        echo "  Global backoff: ${REMAINING}s remaining"
    else
        echo "  Global backoff: None"
    fi
else
    echo "  Global backoff: None"
fi

echo ""
echo "=== Step Function Status ==="

# Get state machine ARN (find it dynamically)
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?contains(name, 'PdfAccessibilityRemediationWorkflow')].stateMachineArn" \
    --output text 2>/dev/null | head -1)

if [ -n "$STATE_MACHINE_ARN" ]; then
    RUNNING=$(aws stepfunctions list-executions \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --status-filter RUNNING \
        --max-results 100 \
        --query 'length(executions)' \
        --output text 2>/dev/null || echo "0")
    echo "  Running executions: $RUNNING"
else
    echo "  Could not find state machine"
fi

echo ""
echo "=== Summary ==="
TOTAL_PENDING=$((${QUEUE_COUNT:-0} + ${RETRY_COUNT:-0} + ${PDF_COUNT:-0}))
echo "  Total pending: $TOTAL_PENDING"
echo "  Completed: ${RESULT_COUNT:-0}"

sleep 2m
done

