#!/bin/bash
# Sets the /pdf-processing/reconciler-enabled SSM parameter

VALUE="${1:-true}"

aws ssm put-parameter \
  --name "/pdf-processing/reconciler-enabled" \
  --value "$VALUE" \
  --type "String" \
  --overwrite

echo "Set /pdf-processing/reconciler-enabled to: $VALUE"
