#!/bin/bash
#
# Set the max pages per chunk when risk-based splitting is disabled
#
# Usage: ./bin/set-splitting-page-count.sh [page_count]
#
# Default: 95 pages per chunk
#
# This parameter is only used when risk-based splitting is disabled.
# When risk-based splitting is enabled, the pre-scan determines chunk size.
#

VALUE="${1:-95}"

if ! [[ "$VALUE" =~ ^[0-9]+$ ]]; then
    echo "Usage: $0 [page_count]"
    echo "  page_count = Max pages per chunk (default: 95)"
    exit 1
fi

aws ssm put-parameter \
    --name "/pdf-processing/splitting-page-count" \
    --value "$VALUE" \
    --type String \
    --overwrite

echo "Splitting page count set to: $VALUE pages per chunk"
