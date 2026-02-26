#!/bin/bash
# Check the current state of the Adobe API in-flight tracking system
# Useful for diagnosing stuck queue issues

echo "=== Adobe API In-Flight Status ==="
echo ""

# Get the in-flight counter
echo "1. In-Flight Counter:"
aws dynamodb get-item \
    --table-name adobe-api-in-flight-tracker \
    --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
    --query 'Item.{in_flight: in_flight.N, last_updated: last_updated.S}' \
    --output table 2>/dev/null || echo "   (table not found or empty)"

echo ""

# Count tracked files (not released)
echo "2. Tracked Files (not released):"
aws dynamodb scan \
    --table-name adobe-api-in-flight-tracker \
    --filter-expression "begins_with(counter_id, :prefix) AND attribute_not_exists(released)" \
    --expression-attribute-values '{":prefix": {"S": "file_"}}' \
    --select COUNT \
    --query 'Count' \
    --output text 2>/dev/null || echo "   0"

echo ""

# Show recent tracked files
echo "3. Recent Tracked Files (last 10):"
aws dynamodb scan \
    --table-name adobe-api-in-flight-tracker \
    --filter-expression "begins_with(counter_id, :prefix) AND attribute_not_exists(released)" \
    --expression-attribute-values '{":prefix": {"S": "file_"}}' \
    --query 'Items[*].{filename: filename.S, api_type: api_type.S, started_at: started_at.S}' \
    --output table 2>/dev/null | head -20 || echo "   (none)"

echo ""

# Check running Step Functions
echo "4. Running Step Function Executions:"
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?contains(name, 'PdfRemediation')].stateMachineArn" --output text 2>/dev/null | head -1)
if [ -n "$STATE_MACHINE_ARN" ]; then
    aws stepfunctions list-executions \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --status-filter RUNNING \
        --query 'length(executions)' \
        --output text 2>/dev/null || echo "   0"
else
    echo "   (state machine not found)"
fi

echo ""

# Check global backoff
echo "5. Global Backoff Status:"
aws dynamodb get-item \
    --table-name adobe-api-in-flight-tracker \
    --key '{"counter_id": {"S": "global_backoff_until"}}' \
    --query 'Item.{backoff_until: backoff_until.N, set_at: set_at.S}' \
    --output table 2>/dev/null || echo "   (no active backoff)"

echo ""

# Check queue folder
echo "6. Files in queue/ folder:"
BUCKET=$(aws ssm get-parameter --name "/pdf-processing/bucket-name" --query 'Parameter.Value' --output text 2>/dev/null || echo "")
if [ -n "$BUCKET" ]; then
    aws s3 ls "s3://$BUCKET/queue/" --recursive 2>/dev/null | wc -l || echo "   0"
else
    echo "   (bucket not configured in SSM)"
fi

echo ""
echo "=== Diagnosis ==="
IN_FLIGHT=$(aws dynamodb get-item --table-name adobe-api-in-flight-tracker --key '{"counter_id": {"S": "adobe_api_in_flight"}}' --query 'Item.in_flight.N' --output text 2>/dev/null || echo "0")
TRACKED=$(aws dynamodb scan --table-name adobe-api-in-flight-tracker --filter-expression "begins_with(counter_id, :prefix) AND attribute_not_exists(released)" --expression-attribute-values '{":prefix": {"S": "file_"}}' --select COUNT --query 'Count' --output text 2>/dev/null || echo "0")

if [ "$IN_FLIGHT" != "None" ] && [ "$IN_FLIGHT" -gt 0 ] && [ "$TRACKED" -eq 0 ]; then
    echo "⚠️  STUCK COUNTER DETECTED: in_flight=$IN_FLIGHT but no tracked files"
    echo "   Run: bin/reset-AIFRRT-in-flight-value-to-zero.sh"
elif [ "$IN_FLIGHT" != "None" ] && [ "$IN_FLIGHT" -gt "$TRACKED" ]; then
    DRIFT=$((IN_FLIGHT - TRACKED))
    echo "⚠️  COUNTER DRIFT: in_flight=$IN_FLIGHT, tracked=$TRACKED (drift=$DRIFT)"
    echo "   The reconciler should fix this automatically within 5 minutes"
else
    echo "✅ Counter appears healthy: in_flight=$IN_FLIGHT, tracked=$TRACKED"
fi
