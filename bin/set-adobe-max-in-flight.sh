#!/bin/bash
# Script to update the Adobe API max in-flight limit

set -e

PARAM_NAME="/pdf-processing/adobe-api-max-in-flight"
CURRENT_VALUE=$(aws ssm get-parameter --name "$PARAM_NAME" --query 'Parameter.Value' --output text 2>/dev/null || echo "150")

echo "Adobe API Max In-Flight Configuration"
echo "======================================"
echo ""
echo "Current max in-flight: $CURRENT_VALUE"
echo ""
echo "Recommended values:"
echo "  100 - Conservative (safe margin, slower throughput)"
echo "  150 - Balanced (default, good for most cases)"
echo "  180 - Aggressive (higher throughput, closer to Adobe's 200 RPM limit)"
echo ""
read -p "Enter new max in-flight value (or press Enter to keep current): " NEW_VALUE

if [ -z "$NEW_VALUE" ]; then
    echo "Keeping current value: $CURRENT_VALUE"
    exit 0
fi

# Validate input is a number
if ! [[ "$NEW_VALUE" =~ ^[0-9]+$ ]]; then
    echo "Error: Please enter a valid number"
    exit 1
fi

# Warn if value is too high
if [ "$NEW_VALUE" -gt 190 ]; then
    echo ""
    echo "WARNING: Value $NEW_VALUE is very close to Adobe's 200 RPM limit."
    echo "This may still result in occasional 429 errors."
    read -p "Are you sure you want to continue? (y/N): " CONFIRM
    if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "Updating SSM parameter..."
aws ssm put-parameter \
    --name "$PARAM_NAME" \
    --value "$NEW_VALUE" \
    --type String \
    --overwrite

echo ""
echo "✓ Max in-flight updated to: $NEW_VALUE"
echo ""
echo "Note: Running ECS tasks will pick up the new value within 5 minutes."
echo "New tasks will use the new value immediately."
