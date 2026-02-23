#!/bin/bash
# Script to reset the Adobe API in-flight counter
# Use this if the counter gets stuck due to crashed tasks

set -e

TABLE_NAME="adobe-api-rate-limit"
COUNTER_ID="adobe_api_in_flight"

echo "Adobe API In-Flight Counter Reset"
echo "=================================="
echo ""

# Get current value
CURRENT=$(aws dynamodb get-item \
    --table-name "$TABLE_NAME" \
    --key "{\"counter_id\": {\"S\": \"$COUNTER_ID\"}}" \
    --query 'Item.in_flight.N' \
    --output text 2>/dev/null || echo "0")

if [ "$CURRENT" == "None" ]; then
    CURRENT="0"
fi

echo "Current in-flight count: $CURRENT"
echo ""

if [ "$CURRENT" == "0" ]; then
    echo "Counter is already at 0. No reset needed."
    exit 0
fi

echo "WARNING: Only reset this counter if you're sure no API calls are in progress."
echo "Resetting while calls are in progress may cause the counter to go negative."
echo ""
read -p "Are you sure you want to reset the counter to 0? (y/N): " CONFIRM

if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Resetting counter..."
aws dynamodb put-item \
    --table-name "$TABLE_NAME" \
    --item "{
        \"counter_id\": {\"S\": \"$COUNTER_ID\"},
        \"in_flight\": {\"N\": \"0\"},
        \"last_updated\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}
    }"

echo ""
echo "✓ In-flight counter reset to 0"
