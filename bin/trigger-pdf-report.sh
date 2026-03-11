#!/bin/bash

echo "Checking AWS identity..."
echo ""
aws sts get-caller-identity
echo ""

read -p "Is this the correct AWS identity? [Y/n] " response
response=${response:-Y}

if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 1
fi

echo ""
echo "Finding PDF Report Generator Lambda..."
FUNCTION_NAME=$(aws lambda list-functions --query "Functions[?contains(FunctionName, 'PdfReportGenerator')].FunctionName" --output text)

if [ -z "$FUNCTION_NAME" ]; then
    echo "Error: Could not find PdfReportGenerator Lambda function."
    exit 1
fi

echo "Found function: $FUNCTION_NAME"
echo ""
echo "Triggering Lambda..."
aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    /tmp/pdf-report-response.json

echo ""
echo "Initial response:"
cat /tmp/pdf-report-response.json
echo ""

# Check if batch processing was started (status 202 means more batches coming)
if grep -q '"statusCode": 202' /tmp/pdf-report-response.json 2>/dev/null || grep -q '"statusCode":202' /tmp/pdf-report-response.json 2>/dev/null; then
    echo ""
    echo "Batch processing started. Monitoring for completion..."
    echo "(Press Ctrl+C to stop monitoring - report will still generate in background)"
    echo ""
    
    LOG_GROUP="/aws/lambda/$FUNCTION_NAME"
    START_TIME=$(date +%s)
    
    while true; do
        # Check logs for completion or error
        RECENT_LOGS=$(aws logs filter-log-events \
            --log-group-name "$LOG_GROUP" \
            --start-time $((START_TIME * 1000)) \
            --filter-pattern "?\"Excel report saved\" ?\"[ERROR]\"" \
            --query 'events[*].message' \
            --output text 2>/dev/null)
        
        if echo "$RECENT_LOGS" | grep -q "Excel report saved"; then
            echo ""
            echo "✓ Report generation complete!"
            echo "$RECENT_LOGS" | grep "Excel report saved"
            break
        fi
        
        if echo "$RECENT_LOGS" | grep -q "\[ERROR\]"; then
            echo ""
            echo "✗ Error detected:"
            echo "$RECENT_LOGS" | grep -A2 "\[ERROR\]"
            exit 1
        fi
        
        # Show progress
        PROGRESS=$(aws logs filter-log-events \
            --log-group-name "$LOG_GROUP" \
            --start-time $((START_TIME * 1000)) \
            --filter-pattern "Processing" \
            --query 'events[-1].message' \
            --output text 2>/dev/null | tail -1)
        
        if [ -n "$PROGRESS" ] && [ "$PROGRESS" != "None" ]; then
            echo -ne "\r$PROGRESS"
        fi
        
        sleep 5
    done
else
    echo "Report generation complete (single batch)."
fi
