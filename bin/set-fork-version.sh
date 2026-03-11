#!/bin/bash
#
# Set the custom fork version displayed in the PDF Processing Throughput widget
#
# Usage:
#   ./bin/set-fork-version.sh [version]
#
# If no version is provided, defaults to 20260114080000
#

PARAM_NAME="/pdf-processing/custom-fork-version"
DEFAULT_VERSION="20260114080000"

VERSION="${1:-$DEFAULT_VERSION}"

echo "Setting custom fork version to: $VERSION"
echo ""

aws ssm put-parameter \
    --name "$PARAM_NAME" \
    --value "$VERSION" \
    --type String \
    --overwrite

if [ $? -eq 0 ]; then
    echo ""
    echo "Successfully updated $PARAM_NAME to $VERSION"
    echo ""
    echo "The change will be reflected in the dashboard widget on next refresh."
else
    echo ""
    echo "Failed to update parameter"
    exit 1
fi
