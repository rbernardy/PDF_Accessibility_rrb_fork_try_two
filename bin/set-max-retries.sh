#!/bin/bash
# Set the maximum retry count for failed PDFs
# After this many failures, PDFs move to failed/ folder instead of queue/
#
# Usage: ./bin/set-max-retries.sh [count]
# Default: 3

MAX_RETRIES=${1:-3}

echo "Setting max retries to: $MAX_RETRIES"

aws ssm put-parameter \
    --name "/pdf-processing/max-retries" \
    --value "$MAX_RETRIES" \
    --type String \
    --overwrite

echo "Done. PDFs will now be retried $MAX_RETRIES times before moving to failed/ folder."
