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
echo "Proceeding with LIVE deployment..."
cdk deploy PDFAccessibility -c source_bucket=pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog -c destination_bucket=usflibraries-pdfaccessibility-live-public --require-approval never
