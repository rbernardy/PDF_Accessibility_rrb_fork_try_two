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

echo "Clearing Docker build cache to force image rebuild..."
docker builder prune -af 2>/dev/null || true
docker system prune -af 2>/dev/null || true

echo ""
echo "Starting CDK deployment with forced image rebuild..."
echo "Build timestamp will be embedded in Docker image to ensure new code is deployed."
cdk deploy PDFAccessibility -c source_bucket=pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog -c destination_bucket=usflibraries-pdfaccessibility-public --require-approval never --force

echo ""
echo "Deployment completed at: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "IMPORTANT: After deployment, verify the new image is running:"
echo "1. Check ECR for new image with recent timestamp"
echo "2. Check ECS task definition points to new image URI"
echo "3. If tasks are still using old code, stop them in ECS console"
echo ""
echo "To verify rate limiter is working, check CloudWatch logs for:"
echo "  - 'Initial jitter:' messages (proves new code is running)"
echo "  - 'RPM (global)' messages (proves combined counter is active)"
