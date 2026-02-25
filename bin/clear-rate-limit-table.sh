#!/bin/bash
#
# Clear the Adobe API rate limit DynamoDB table
# This removes all in-flight tracking, RPM counters, and file entries
#
# Usage: ./bin/clear-rate-limit-table.sh [table-name]
#
# If table-name is not provided, defaults to "adobe-api-rate-limit"
#

set -e

TABLE_NAME="${1:-adobe-api-rate-limit}"

echo "=== Clearing Rate Limit Table: $TABLE_NAME ==="
echo ""

# Check if table exists
if ! aws dynamodb describe-table --table-name "$TABLE_NAME" > /dev/null 2>&1; then
    echo "ERROR: Table '$TABLE_NAME' does not exist"
    echo "Usage: $0 [table-name]"
    exit 1
fi

echo "Step 1: Resetting in-flight counter to 0..."
aws dynamodb put-item \
    --table-name "$TABLE_NAME" \
    --item '{
        "counter_id": {"S": "adobe_api_in_flight"},
        "in_flight": {"N": "0"},
        "last_updated": {"S": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}
    }'
echo "  ✓ In-flight counter reset to 0"

echo ""
echo "Step 2: Removing global backoff (if any)..."
aws dynamodb delete-item \
    --table-name "$TABLE_NAME" \
    --key '{"counter_id": {"S": "global_backoff_until"}}' 2>/dev/null || true
echo "  ✓ Global backoff cleared"

echo ""
echo "Step 3: Scanning for file tracking entries (file_*)..."
FILE_ITEMS=$(aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --filter-expression "begins_with(counter_id, :prefix)" \
    --expression-attribute-values '{":prefix": {"S": "file_"}}' \
    --projection-expression "counter_id" \
    --output json)

FILE_COUNT=$(echo "$FILE_ITEMS" | jq '.Items | length')
echo "  Found $FILE_COUNT file tracking entries"

if [ "$FILE_COUNT" -gt 0 ]; then
    echo "  Deleting file entries..."
    echo "$FILE_ITEMS" | jq -r '.Items[].counter_id.S' | while read -r counter_id; do
        aws dynamodb delete-item \
            --table-name "$TABLE_NAME" \
            --key "{\"counter_id\": {\"S\": \"$counter_id\"}}"
        echo "    Deleted: $counter_id"
    done
    echo "  ✓ All file tracking entries deleted"
else
    echo "  ✓ No file tracking entries to delete"
fi

echo ""
echo "Step 4: Scanning for RPM window entries (rpm_window_*)..."
RPM_ITEMS=$(aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --filter-expression "begins_with(counter_id, :prefix)" \
    --expression-attribute-values '{":prefix": {"S": "rpm_window_"}}' \
    --projection-expression "counter_id" \
    --output json)

RPM_COUNT=$(echo "$RPM_ITEMS" | jq '.Items | length')
echo "  Found $RPM_COUNT RPM window entries"

if [ "$RPM_COUNT" -gt 0 ]; then
    echo "  Deleting RPM entries..."
    echo "$RPM_ITEMS" | jq -r '.Items[].counter_id.S' | while read -r counter_id; do
        aws dynamodb delete-item \
            --table-name "$TABLE_NAME" \
            --key "{\"counter_id\": {\"S\": \"$counter_id\"}}"
    done
    echo "  ✓ All RPM window entries deleted"
else
    echo "  ✓ No RPM window entries to delete"
fi

echo ""
echo "Step 5: Scanning for RPS window entries (rps_window_*)..."
RPS_ITEMS=$(aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --filter-expression "begins_with(counter_id, :prefix)" \
    --expression-attribute-values '{":prefix": {"S": "rps_window_"}}' \
    --projection-expression "counter_id" \
    --output json)

RPS_COUNT=$(echo "$RPS_ITEMS" | jq '.Items | length')
echo "  Found $RPS_COUNT RPS window entries"

if [ "$RPS_COUNT" -gt 0 ]; then
    echo "  Deleting RPS entries..."
    echo "$RPS_ITEMS" | jq -r '.Items[].counter_id.S' | while read -r counter_id; do
        aws dynamodb delete-item \
            --table-name "$TABLE_NAME" \
            --key "{\"counter_id\": {\"S\": \"$counter_id\"}}"
    done
    echo "  ✓ All RPS window entries deleted"
else
    echo "  ✓ No RPS window entries to delete"
fi

echo ""
echo "=== Rate Limit Table Cleared ==="
echo ""
echo "Current state:"
aws dynamodb get-item \
    --table-name "$TABLE_NAME" \
    --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
    --output json | jq '.Item'
