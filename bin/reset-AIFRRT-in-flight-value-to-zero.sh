#!/bin/bash
# Reset the Adobe API in-flight counter to zero in DynamoDB
# Use this when the counter gets stuck due to failed tasks not releasing their slots

aws dynamodb update-item \
    --table-name adobe-api-in-flight-tracker \
    --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
    --update-expression "SET in_flight = :zero, last_updated = :now" \
    --expression-attribute-values '{":zero": {"N": "0"}, ":now": {"S": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}}' \
    --return-values UPDATED_NEW

echo "In-flight counter reset to zero"
