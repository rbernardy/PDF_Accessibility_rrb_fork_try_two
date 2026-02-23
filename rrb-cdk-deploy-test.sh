#!/bin/bash

echo "Checking AWS identity..."
echo ""
aws sts get-caller-identity
echo ""

read -p "Is this the correct AWS identity? [Y/n] " response
response=${response:-Y}

if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 1
fi

echo ""
echo "Proceeding with TEST deployment..."
echo "Cleaning CDK output directory..."
rm -rf ./cdk.out/

echo "Clearing Docker build cache..."
docker builder prune -f 2>/dev/null || true

echo "Starting CDK deployment..."
cdk deploy PDFAccessibility -c source_bucket=pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog -c destination_bucket=usflibraries-pdfaccessibility-public --require-approval never --force

echo ""
echo "Deployment completed at: $(date '+%Y-%m-%d %H:%M:%S')"
