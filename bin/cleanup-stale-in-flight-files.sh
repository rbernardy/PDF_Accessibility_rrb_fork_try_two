#!/bin/bash
# Clean up stale in-flight file entries from DynamoDB
# Use this when files show as "in-flight" but no processing is actually happening

TABLE_NAME="adobe-api-in-flight-tracker"

echo "Scanning for stale file entries..."

# Get all file_ entries
aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --filter-expression "begins_with(counter_id, :prefix)" \
    --expression-attribute-values '{":prefix": {"S": "file_"}}' \
    --projection-expression "counter_id" \
    --output json > /tmp/stale_files.json

# Count entries
COUNT=$(jq '.Items | length' /tmp/stale_files.json)
echo "Found $COUNT stale file entries"

if [ "$COUNT" -eq "0" ]; then
    echo "No stale entries to clean up"
    exit 0
fi

echo "Deleting stale entries..."

# Delete each entry
jq -r '.Items[].counter_id.S' /tmp/stale_files.json | while read -r counter_id; do
    aws dynamodb delete-item \
        --table-name "$TABLE_NAME" \
        --key "{\"counter_id\": {\"S\": \"$counter_id\"}}" \
        --output text
    echo "  Deleted: $counter_id"
done

# Also reset the in-flight counter to 0
aws dynamodb update-item \
    --table-name "$TABLE_NAME" \
    --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
    --update-expression "SET in_flight = :zero, last_updated = :now" \
    --expression-attribute-values '{":zero": {"N": "0"}, ":now": {"S": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}}' \
    --return-values UPDATED_NEW

echo ""
echo "Cleanup complete. Deleted $COUNT stale file entries and reset counter to 0."
