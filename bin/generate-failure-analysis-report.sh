#!/bin/bash
# Manually trigger the failure analysis report generator lambda

FUNCTION_NAME="failure-analysis-report"

echo "Invoking $FUNCTION_NAME lambda..."

aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    /tmp/failure-analysis-report-response.json

echo ""
echo "Response:"
cat /tmp/failure-analysis-report-response.json
echo ""

# Clean up
rm -f /tmp/failure-analysis-report-response.json
