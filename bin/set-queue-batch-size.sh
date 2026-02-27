#!/bin/bash
# Set the queue batch size parameter
# This controls how many PDFs are picked up from the queue per batch
#
# Usage: ./bin/set-queue-batch-size.sh [size]

set -e

PARAM_NAME="/pdf-processing/queue-batch-size"
CURRENT_VALUE=$(aws ssm get-parameter --name "$PARAM_NAME" --query 'Parameter.Value' --output text 2>/dev/null || echo "not set")

echo "Queue Batch Size Configuration"
echo "==============================="
echo "Current value: $CURRENT_VALUE"
echo ""
echo "Recommended values:"
echo "  5-8   - Conservative (lower memory usage, slower throughput)"
echo "  10-15 - Balanced (good for most cases)"
echo "  20+   - Aggressive (higher throughput, more concurrent processing)"
echo ""

if [ -n "$1" ]; then
    NEW_VALUE="$1"
else
    read -p "Enter new batch size (or press Enter to keep current): " NEW_VALUE
fi

if [ -z "$NEW_VALUE" ]; then
    echo "Keeping current value: $CURRENT_VALUE"
    exit 0
fi

# Validate input is a number
if ! [[ "$NEW_VALUE" =~ ^[0-9]+$ ]]; then
    echo "Error: Please enter a valid number"
    exit 1
fi

if [ "$NEW_VALUE" -lt 1 ]; then
    echo "Error: Batch size must be at least 1"
    exit 1
fi

echo ""
echo "Setting queue-batch-size to: $NEW_VALUE"
aws ssm put-parameter \
    --name "$PARAM_NAME" \
    --value "$NEW_VALUE" \
    --type String \
    --overwrite

echo "✓ Queue batch size updated to: $NEW_VALUE"
echo ""
echo "Note: New value takes effect on the next queue processing cycle."
