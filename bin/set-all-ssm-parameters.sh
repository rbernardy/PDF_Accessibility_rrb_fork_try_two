#!/bin/bash
#
# Set all PDF processing SSM parameters
# This script allows you to configure all parameters interactively or with defaults
#
# Usage: 
#   ./bin/set-all-ssm-parameters.sh           # Interactive mode
#   ./bin/set-all-ssm-parameters.sh --defaults # Use recommended defaults
#

set -e

# Default values
DEFAULT_QUEUE_ENABLED="true"
DEFAULT_QUEUE_BATCH_SIZE="8"
DEFAULT_QUEUE_BATCH_SIZE_LOW_LOAD="15"
DEFAULT_QUEUE_MAX_IN_FLIGHT="12"
DEFAULT_QUEUE_MAX_EXECUTIONS="75"
DEFAULT_ADOBE_API_MAX_IN_FLIGHT="150"
DEFAULT_ADOBE_API_RPM="200"
DEFAULT_ADOBE_API_RPS="10"
DEFAULT_MAX_RETRIES="3"
DEFAULT_RECONCILER_ENABLED="true"
DEFAULT_RECONCILER_MAX_DRIFT="5"
DEFAULT_EMAIL_ENABLED="false"
DEFAULT_SENDER_EMAIL=""

USE_DEFAULTS=false

if [ "$1" == "--defaults" ]; then
    USE_DEFAULTS=true
fi

echo "========================================"
echo "PDF Processing SSM Parameter Setup"
echo "========================================"
echo ""

# Function to get current value or default
get_current() {
    aws ssm get-parameter --name "$1" --query 'Parameter.Value' --output text 2>/dev/null || echo "$2"
}

# Function to set parameter
set_param() {
    local name="$1"
    local value="$2"
    local description="$3"
    
    aws ssm put-parameter \
        --name "$name" \
        --value "$value" \
        --type String \
        --overwrite > /dev/null
    echo "  ✓ $name = $value"
}

# Function to prompt for value
prompt_value() {
    local name="$1"
    local current="$2"
    local default="$3"
    local description="$4"
    
    if [ "$USE_DEFAULTS" = true ]; then
        echo "$default"
        return
    fi
    
    echo ""
    echo "$description"
    echo "  Current: $current"
    echo "  Default: $default"
    read -p "  Enter value (or press Enter for default): " value
    
    if [ -z "$value" ]; then
        echo "$default"
    else
        echo "$value"
    fi
}

echo "Fetching current values..."
echo ""

# Queue Processing Parameters
echo "--- Queue Processing ---"

CURRENT=$(get_current "/pdf-processing/queue-enabled" "$DEFAULT_QUEUE_ENABLED")
VALUE=$(prompt_value "/pdf-processing/queue-enabled" "$CURRENT" "$DEFAULT_QUEUE_ENABLED" "queue-enabled: Enable/disable queue processing (true/false)")
set_param "/pdf-processing/queue-enabled" "$VALUE"

CURRENT=$(get_current "/pdf-processing/queue-batch-size" "$DEFAULT_QUEUE_BATCH_SIZE")
VALUE=$(prompt_value "/pdf-processing/queue-batch-size" "$CURRENT" "$DEFAULT_QUEUE_BATCH_SIZE" "queue-batch-size: PDFs to process per batch (5-20)")
set_param "/pdf-processing/queue-batch-size" "$VALUE"

CURRENT=$(get_current "/pdf-processing/queue-batch-size-low-load" "$DEFAULT_QUEUE_BATCH_SIZE_LOW_LOAD")
VALUE=$(prompt_value "/pdf-processing/queue-batch-size-low-load" "$CURRENT" "$DEFAULT_QUEUE_BATCH_SIZE_LOW_LOAD" "queue-batch-size-low-load: PDFs per batch when system load is low (10-25)")
set_param "/pdf-processing/queue-batch-size-low-load" "$VALUE"

CURRENT=$(get_current "/pdf-processing/queue-max-in-flight" "$DEFAULT_QUEUE_MAX_IN_FLIGHT")
VALUE=$(prompt_value "/pdf-processing/queue-max-in-flight" "$CURRENT" "$DEFAULT_QUEUE_MAX_IN_FLIGHT" "queue-max-in-flight: Skip queue if in-flight exceeds this (8-20)")
set_param "/pdf-processing/queue-max-in-flight" "$VALUE"

CURRENT=$(get_current "/pdf-processing/queue-max-executions" "$DEFAULT_QUEUE_MAX_EXECUTIONS")
VALUE=$(prompt_value "/pdf-processing/queue-max-executions" "$CURRENT" "$DEFAULT_QUEUE_MAX_EXECUTIONS" "queue-max-executions: Skip queue if running executions exceed this (50-100)")
set_param "/pdf-processing/queue-max-executions" "$VALUE"

# Adobe API Parameters
echo ""
echo "--- Adobe API Rate Limiting ---"

CURRENT=$(get_current "/pdf-processing/adobe-api-max-in-flight" "$DEFAULT_ADOBE_API_MAX_IN_FLIGHT")
VALUE=$(prompt_value "/pdf-processing/adobe-api-max-in-flight" "$CURRENT" "$DEFAULT_ADOBE_API_MAX_IN_FLIGHT" "adobe-api-max-in-flight: Max concurrent Adobe API calls (100-180)")
set_param "/pdf-processing/adobe-api-max-in-flight" "$VALUE"

CURRENT=$(get_current "/pdf-processing/adobe-api-rpm" "$DEFAULT_ADOBE_API_RPM")
VALUE=$(prompt_value "/pdf-processing/adobe-api-rpm" "$CURRENT" "$DEFAULT_ADOBE_API_RPM" "adobe-api-rpm: Adobe API requests per minute limit (150-200)")
set_param "/pdf-processing/adobe-api-rpm" "$VALUE"

CURRENT=$(get_current "/pdf-processing/adobe-api-rps" "$DEFAULT_ADOBE_API_RPS")
VALUE=$(prompt_value "/pdf-processing/adobe-api-rps" "$CURRENT" "$DEFAULT_ADOBE_API_RPS" "adobe-api-rps: Adobe API requests per second limit (5-15)")
set_param "/pdf-processing/adobe-api-rps" "$VALUE"

# Retry Parameters
echo ""
echo "--- Retry Configuration ---"

CURRENT=$(get_current "/pdf-processing/max-retries" "$DEFAULT_MAX_RETRIES")
VALUE=$(prompt_value "/pdf-processing/max-retries" "$CURRENT" "$DEFAULT_MAX_RETRIES" "max-retries: Times to retry failed PDFs before moving to failed/ (2-5)")
set_param "/pdf-processing/max-retries" "$VALUE"

# Reconciler Parameters
echo ""
echo "--- In-Flight Reconciler ---"

CURRENT=$(get_current "/pdf-processing/reconciler-enabled" "$DEFAULT_RECONCILER_ENABLED")
VALUE=$(prompt_value "/pdf-processing/reconciler-enabled" "$CURRENT" "$DEFAULT_RECONCILER_ENABLED" "reconciler-enabled: Auto-fix counter drift (true/false)")
set_param "/pdf-processing/reconciler-enabled" "$VALUE"

CURRENT=$(get_current "/pdf-processing/reconciler-max-drift" "$DEFAULT_RECONCILER_MAX_DRIFT")
VALUE=$(prompt_value "/pdf-processing/reconciler-max-drift" "$CURRENT" "$DEFAULT_RECONCILER_MAX_DRIFT" "reconciler-max-drift: Max drift before auto-reset (3-10)")
set_param "/pdf-processing/reconciler-max-drift" "$VALUE"

# Email/Digest Parameters
echo ""
echo "--- Email Digest ---"

CURRENT=$(get_current "/pdf-processing/email-enabled" "$DEFAULT_EMAIL_ENABLED")
VALUE=$(prompt_value "/pdf-processing/email-enabled" "$CURRENT" "$DEFAULT_EMAIL_ENABLED" "email-enabled: Enable daily failure digest emails (true/false)")
set_param "/pdf-processing/email-enabled" "$VALUE"

CURRENT=$(get_current "/pdf-processing/sender-email" "$DEFAULT_SENDER_EMAIL")
if [ "$USE_DEFAULTS" = false ]; then
    echo ""
    echo "sender-email: SES verified email for sending digests"
    echo "  Current: $CURRENT"
    read -p "  Enter email (or press Enter to skip): " VALUE
    if [ -n "$VALUE" ]; then
        set_param "/pdf-processing/sender-email" "$VALUE"
    else
        echo "  (skipped)"
    fi
fi

echo ""
echo "========================================"
echo "All parameters configured!"
echo "========================================"
echo ""
echo "View current values with: ./bin/view-current-ssm-parameters.sh"
