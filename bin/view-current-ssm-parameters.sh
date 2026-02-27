#!/bin/bash
echo "=== AWS Account Info ==="
aws sts get-caller-identity
echo ""
echo "=== PDF Processing SSM Parameters ==="
aws ssm get-parameters-by-path --path "/pdf-processing/" --query "Parameters[*].[Name,Value]" --output table
