#!/bin/bash
# Set the sender email SSM parameter for PDF failure digest notifications

EMAIL="${1:-lib-systems@usf.edu}"

echo "Setting /pdf-processing/sender-email to: $EMAIL"

aws ssm put-parameter \
    --name "/pdf-processing/sender-email" \
    --value "$EMAIL" \
    --type String \
    --overwrite

if [ $? -eq 0 ]; then
    echo "✓ Sender email set successfully"
else
    echo "✗ Failed to set sender email"
    exit 1
fi
