#!/bin/bash
# Set the queue batch size for low load conditions
#
# Usage: ./bin/set-queue-batch-size-low-load.sh <value>
#
# This controls how many files are moved from queue/ to pdf/ per invocation
# when the system is under low load (< 10 running executions AND < 3 in-flight)

if [ -z "$1" ]; then
    echo "Usage: $0 <value>"
    echo "Example: $0 60"
    exit 1
fi

VALUE="$1"

echo "Setting /pdf-processing/queue-batch-size-low-load to: $VALUE"

aws ssm put-parameter \
    --name "/pdf-processing/queue-batch-size-low-load" \
    --value "$VALUE" \
    --type String \
    --overwrite

if [ $? -eq 0 ]; then
    echo "✓ queue-batch-size-low-load set successfully"
else
    echo "✗ Failed to set parameter"
    exit 1
fi
