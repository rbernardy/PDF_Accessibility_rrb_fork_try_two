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
    /tmp/pdf-report-response.json

echo ""
echo "Response:"
cat /tmp/pdf-report-response.json
echo ""
