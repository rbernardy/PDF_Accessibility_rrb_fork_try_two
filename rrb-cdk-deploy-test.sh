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
cdk deploy PDFAccessibility -c source_bucket=pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog -c destination_bucket=usflibraries-pdfaccessibility-public --require-approval never --force
