#!/bin/bash
#
# Enable or disable risk-based PDF splitting
#
# Usage: ./bin/set-risk-based-splitting.sh [true|false]
#
# When enabled (true - default):
#   - HIGH-RISK PDFs are moved to pre-failed/ and NOT processed
#   - MEDIUM-RISK PDFs use smaller chunks
#   - LOW-RISK PDFs are processed normally
#
# When disabled (false):
#   - All PDFs are processed using page count and file size limits only
#   - No PDFs are moved to pre-failed/
#   - Pre-scan data is still collected for analysis
#

VALUE="${1:-true}"

if [[ "$VALUE" != "true" && "$VALUE" != "false" ]]; then
    echo "Usage: $0 [true|false]"
    echo "  true  = Enable risk-based splitting (default)"
    echo "  false = Disable risk-based splitting (split by page count only)"
    exit 1
fi

aws ssm put-parameter \
    --name "/pdf-processing/risk-based-splitting-enabled" \
    --value "$VALUE" \
    --type String \
    --overwrite

echo "Risk-based splitting set to: $VALUE"
