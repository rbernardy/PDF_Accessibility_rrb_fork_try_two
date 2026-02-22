#!/bin/bash

# Script to view or update the Adobe API RPM limit

PARAM_NAME="/pdf-processing/adobe-api-rpm"

echo "Checking AWS identity..."
echo ""
aws sts get-caller-identity
echo ""

read -p "Is this the correct AWS identity? [Y/n] " response
response=${response:-Y}

if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 1
fi

echo ""

# Get current value
echo "Current Adobe API RPM setting:"
aws ssm get-parameter --name "$PARAM_NAME" --query 'Parameter.Value' --output text 2>/dev/null || echo "Parameter not found (will use default: 200)"
echo ""

# Check if user wants to update
read -p "Enter new RPM value (or press Enter to keep current): " new_rpm

if [ -n "$new_rpm" ]; then
    # Validate it's a number
    if ! [[ "$new_rpm" =~ ^[0-9]+$ ]]; then
        echo "Error: RPM must be a positive integer"
        exit 1
    fi
    
    echo "Setting Adobe API RPM to: $new_rpm"
    aws ssm put-parameter \
        --name "$PARAM_NAME" \
        --value "$new_rpm" \
        --type String \
        --overwrite
    
    echo "Done! New RPM limit: $new_rpm"
    echo ""
    echo "Note: Running ECS tasks will pick up the new value within 5 minutes."
else
    echo "No changes made."
fi
