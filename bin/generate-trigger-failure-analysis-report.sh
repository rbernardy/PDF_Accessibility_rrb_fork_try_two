#!/bin/bash
# Manually trigger the failure analysis report generator lambda

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

FUNCTION_NAME="failure-analysis-report"

echo ""
echo "Checking if Lambda exists..."
if ! aws lambda get-function --function-name "$FUNCTION_NAME" > /dev/null 2>&1; then
    echo "Error: Lambda function '$FUNCTION_NAME' not found."
    echo "Make sure the stack is deployed."
    exit 1
fi

echo "Found function: $FUNCTION_NAME"
echo ""
echo "Invoking Lambda (this may take a few minutes)..."

aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    /tmp/failure-analysis-report-response.json

INVOKE_STATUS=$?

echo ""
if [ $INVOKE_STATUS -eq 0 ] && [ -f /tmp/failure-analysis-report-response.json ]; then
    echo "Response:"
    cat /tmp/failure-analysis-report-response.json
    echo ""
    rm -f /tmp/failure-analysis-report-response.json
else
    echo "Lambda invocation may have timed out or failed."
    echo "Check CloudWatch logs for details:"
    echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --since 10m"
fi
