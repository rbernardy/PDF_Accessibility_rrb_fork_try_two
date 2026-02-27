#!/bin/bash
# Set the max running Step Function executions threshold for queue processing
#
# Usage: ./bin/set-queue-max-executions.sh <value>
#
# The queue processor will skip moving files from queue/ to pdf/ if the number
# of running Step Function executions is >= this value.

if [ -z "$1" ]; then
    echo "Usage: $0 <value>"
    echo "Example: $0 75"
    exit 1
fi

VALUE="$1"

echo "Setting /pdf-processing/queue-max-executions to: $VALUE"

aws ssm put-parameter \
    --name "/pdf-processing/queue-max-executions" \
    --value "$VALUE" \
    --type String \
    --overwrite

if [ $? -eq 0 ]; then
    echo "✓ queue-max-executions set successfully"
else
    echo "✗ Failed to set parameter"
    exit 1
fi
