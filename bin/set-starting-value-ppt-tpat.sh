aws dynamodb put-item \
  --table-name adobe-api-in-flight-tracker \
  --item '{"counter_id": {"S": "success_total"}, "count": {"N": "20406"}}'

