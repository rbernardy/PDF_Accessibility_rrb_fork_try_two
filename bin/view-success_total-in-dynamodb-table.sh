
aws dynamodb get-item \
  --table-name adobe-api-in-flight-tracker \
  --key '{"counter_id": {"S": "success_total"}}'
