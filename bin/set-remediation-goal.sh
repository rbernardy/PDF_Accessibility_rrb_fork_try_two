#!/bin/bash
# Set the remediation count goal SSM parameter

PARAM_NAME="/pdf-processing/remediation-count-goal"

if [ -z "$1" ]; then
    echo "Usage: $0 <goal_value>"
    echo "Example: $0 10000"
    echo ""
    echo "Current value:"
    aws ssm get-parameter --name "$PARAM_NAME" --query 'Parameter.Value' --output text 2>/dev/null || echo "  (not set)"
    exit 1
fi

GOAL_VALUE="$1"

# Validate that the value is a positive integer
if ! [[ "$GOAL_VALUE" =~ ^[0-9]+$ ]]; then
    echo "Error: Goal value must be a positive integer"
    exit 1
fi

aws ssm put-parameter \
    --name "$PARAM_NAME" \
    --value "$GOAL_VALUE" \
    --type String \
    --overwrite

echo "Remediation goal set to: $GOAL_VALUE"
