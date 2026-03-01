#!/bin/bash
# Set the max reconciler drift
#
# Usage: ./bin/set-reconciler-max-drift.sh <value>
#

if [ -z "$1" ]; then
    echo "Usage: $0 <value>"
    echo "Example: $0 5"
    exit 1
fi

VALUE="$1"

echo "Setting /pdf-processing/reconciler-max-drift to: $VALUE"

aws ssm put-parameter \
    --name "/pdf-processing/reconciler-max-drift" \
    --value "$VALUE" \
    --type String \
    --overwrite

if [ $? -eq 0 ]; then
    echo "✓ reconciler-max-drift"
else
    echo "✗ Failed to set parameter"
    exit 1
fi
