aws dynamodb update-item \
  --table-name adobe-api-in-flight-tracker \
  --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
  --update-expression "SET in_flight = :zero" \
  --expression-attribute-values '{":zero": {"N": "0"}}'

