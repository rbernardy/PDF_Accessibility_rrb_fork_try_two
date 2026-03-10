#!/bin/bash
# Set the remediation deadline date SSM parameter

PARAM_NAME="/pdf-processing/remediation-deadline-date"

if [ -z "$1" ]; then
    echo "Usage: $0 <date>"
    echo "Example: $0 2026-04-26"
    echo ""
    echo "Current value:"
    aws ssm get-parameter --name "$PARAM_NAME" --query 'Parameter.Value' --output text 2>/dev/null || echo "  (not set)"
    exit 1
fi

DATE_VALUE="$1"

# Validate date format (YYYY-MM-DD)
if ! [[ "$DATE_VALUE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Date must be in YYYY-MM-DD format"
    exit 1
fi

aws ssm put-parameter \
    --name "$PARAM_NAME" \
    --value "$DATE_VALUE" \
    --type String \
    --overwrite

echo "Remediation deadline set to: $DATE_VALUE"
