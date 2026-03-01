#!/bin/bash
#
# Check the status of the PDF processing queue
# Shows counts in queue/, retry/, pdf/, failed/, and result/ folders
# Lists individual files under each folder
# Tracks percentage change between runs
#
# Usage: ./bin/check-queue-status.sh [options]
#
# Options:
#   --bucket NAME       S3 bucket name (default: pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog)
#   --queuelines N      Limit queue folder file listing to N lines
#   --failedlines N     Limit failed folder file listing to N lines
#

set -e

# Default values
BUCKET_NAME="pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog"
QUEUE_LINES="0"
FAILED_LINES="0"

# State file for tracking previous counts
STATE_FILE="/tmp/queue-status-test-state.txt"

# History file for tracking result counts over time (for averages)
HISTORY_FILE="/tmp/queue-status-test-history.txt"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        --queuelines)
            QUEUE_LINES="$2"
            shift 2
            ;;
        --failedlines)
            FAILED_LINES="$2"
            shift 2
            ;;
        *)
            # Legacy: first positional arg is bucket name
            BUCKET_NAME="$1"
            shift
            ;;
    esac
done

# Function to load previous state
load_previous_state() {
    if [ -f "$STATE_FILE" ]; then
        source "$STATE_FILE"
    else
        PREV_QUEUE_COUNT=0
        PREV_PDF_COUNT=0
        PREV_RESULT_COUNT=0
        PREV_FAILED_COUNT=0
        PREV_TOTAL_PENDING=0
        PREV_TIMESTAMP=""
    fi
}

# Function to save current state
save_state() {
    cat > "$STATE_FILE" << EOF
PREV_QUEUE_COUNT=$QUEUE_COUNT
PREV_PDF_COUNT=$PDF_COUNT
PREV_RESULT_COUNT=$RESULT_COUNT
PREV_FAILED_COUNT=$FAILED_COUNT
PREV_TOTAL_PENDING=$TOTAL_PENDING
PREV_TIMESTAMP="$CURRENT_TIMESTAMP"
EOF
}

# Function to append to history file for throughput calculations
append_history() {
    local ts=$(date +%s)
    echo "$ts,$RESULT_COUNT" >> "$HISTORY_FILE"
    
    # Keep only last 7 days of history (clean up old entries)
    local cutoff=$((ts - 604800))  # 7 days in seconds
    if [ -f "$HISTORY_FILE" ]; then
        awk -F',' -v cutoff="$cutoff" '$1 >= cutoff' "$HISTORY_FILE" > "${HISTORY_FILE}.tmp"
        mv "${HISTORY_FILE}.tmp" "$HISTORY_FILE"
    fi
}

# Function to calculate throughput for a time period
# Returns: PDFs processed in that period
calc_throughput() {
    local seconds_ago=$1
    local now=$(date +%s)
    local cutoff=$((now - seconds_ago))
    
    if [ ! -f "$HISTORY_FILE" ]; then
        echo "0,0"
        return
    fi
    
    # Get the oldest entry within the time window
    local oldest_in_window=$(awk -F',' -v cutoff="$cutoff" '$1 >= cutoff {print; exit}' "$HISTORY_FILE")
    # Get the newest entry (current)
    local newest=$(tail -1 "$HISTORY_FILE")
    
    if [ -z "$oldest_in_window" ] || [ -z "$newest" ]; then
        echo "0,0"
        return
    fi
    
    local oldest_count=$(echo "$oldest_in_window" | cut -d',' -f2)
    local newest_count=$(echo "$newest" | cut -d',' -f2)
    local oldest_ts=$(echo "$oldest_in_window" | cut -d',' -f1)
    
    local diff=$((newest_count - oldest_count))
    local time_diff=$((now - oldest_ts))
    
    # Return: processed_count,actual_seconds
    echo "$diff,$time_diff"
}

# Function to calculate and format change
format_change() {
    local current=$1
    local previous=$2
    local name=$3
    
    if [ "$previous" = "" ] || [ "$previous" = "0" ]; then
        echo ""
        return
    fi
    
    local diff=$((current - previous))
    
    if [ $diff -eq 0 ]; then
        echo " (no change)"
    elif [ $diff -gt 0 ]; then
        echo " (+$diff)"
    else
        echo " ($diff)"
    fi
}

while true; do

clear
CURRENT_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
echo "$CURRENT_TIMESTAMP"

# Load previous state
load_previous_state

if [ -n "$PREV_TIMESTAMP" ]; then
    echo "Previous check: $PREV_TIMESTAMP"
fi

echo "Bucket: $BUCKET_NAME"

# Check if queue processing is enabled
QUEUE_ENABLED=$(aws ssm get-parameter \
    --name "/pdf-processing/queue-enabled" \
    --query 'Parameter.Value' \
    --output text 2>/dev/null || echo "true")
if [ "$QUEUE_ENABLED" = "true" ] || [ "$QUEUE_ENABLED" = "1" ]; then
    echo "Queue Processing: ENABLED"
else
    echo "Queue Processing: PAUSED (run ./bin/queue-resume.sh to enable)"
fi

# Count files in each folder
echo "Folder counts:"

# Queue folder
QUEUE_FILES=$(aws s3 ls "s3://${BUCKET_NAME}/queue/" --recursive 2>/dev/null | grep "\.pdf$" || true)
QUEUE_COUNT=$(echo "$QUEUE_FILES" | grep -c "\.pdf$" 2>/dev/null || echo "0")
QUEUE_COUNT=$(echo "$QUEUE_COUNT" | sed 's/^0*//' | tr -d '[:space:]')
QUEUE_COUNT=${QUEUE_COUNT:-0}
QUEUE_CHANGE=$(format_change $QUEUE_COUNT $PREV_QUEUE_COUNT "queue")
echo "  queue/  : $QUEUE_COUNT PDFs (waiting to be processed)$QUEUE_CHANGE"
if [ "$QUEUE_COUNT" -gt 0 ] && [ -n "$QUEUE_FILES" ]; then
    if [ -n "$QUEUE_LINES" ]; then
        echo "$QUEUE_FILES" | head -n "$QUEUE_LINES" | while read -r line; do
            FILE=$(echo "$line" | awk '{print $NF}')
            SIZE=$(echo "$line" | awk '{print $3}' | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')
            DATE=$(echo "$line" | awk '{print $1, $2}')
            echo -e "\t  $FILE ($SIZE bytes, $DATE)"
        done
        if [ "$QUEUE_COUNT" -gt "$QUEUE_LINES" ]; then
            REMAINING=$((QUEUE_COUNT - QUEUE_LINES))
            echo -e "\t  ... and $REMAINING more"
        fi
    else
        echo "$QUEUE_FILES" | while read -r line; do
            FILE=$(echo "$line" | awk '{print $NF}')
            SIZE=$(echo "$line" | awk '{print $3}' | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')
            DATE=$(echo "$line" | awk '{print $1, $2}')
            echo -e "\t  $FILE ($SIZE bytes, $DATE)"
        done
    fi
fi

# Retry folder (legacy)
RETRY_FILES=$(aws s3 ls "s3://${BUCKET_NAME}/retry/" --recursive 2>/dev/null | grep "\.pdf$" || true)
RETRY_COUNT=$(echo "$RETRY_FILES" | grep -c "\.pdf$" 2>/dev/null || echo "0")
RETRY_COUNT=$(echo "$RETRY_COUNT" | sed 's/^0*//' | tr -d '[:space:]')
RETRY_COUNT=${RETRY_COUNT:-0}
echo "  retry/  : $RETRY_COUNT PDFs (legacy retry folder)"
if [ "$RETRY_COUNT" -gt 0 ] && [ -n "$RETRY_FILES" ]; then
    echo "$RETRY_FILES" | while read -r line; do
        FILE=$(echo "$line" | awk '{print $NF}')
        SIZE=$(echo "$line" | awk '{print $3}' | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')
        DATE=$(echo "$line" | awk '{print $1, $2}')
        echo -e "\t  $FILE ($SIZE bytes, $DATE)"
    done
fi

# PDF folder (currently processing) - count only
PDF_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/pdf/" --recursive 2>/dev/null | grep -c "\.pdf$" || echo "0")
PDF_COUNT=$(echo "$PDF_COUNT" | sed 's/^0*//' | tr -d '[:space:]')
PDF_COUNT=${PDF_COUNT:-0}
PDF_CHANGE=$(format_change $PDF_COUNT $PREV_PDF_COUNT "pdf")
echo "  pdf/    : $PDF_COUNT PDFs (currently processing)$PDF_CHANGE"

# Failed folder
FAILED_FILES=$(aws s3 ls "s3://${BUCKET_NAME}/failed/" --recursive 2>/dev/null | grep "\.pdf$" || true)
FAILED_COUNT=$(echo "$FAILED_FILES" | grep -c "\.pdf$" 2>/dev/null || echo "0")
FAILED_COUNT=$(echo "$FAILED_COUNT" | sed 's/^0*//' | tr -d '[:space:]')
FAILED_COUNT=${FAILED_COUNT:-0}
FAILED_CHANGE=$(format_change $FAILED_COUNT $PREV_FAILED_COUNT "failed")
echo "  failed/ : $FAILED_COUNT PDFs (max retries exceeded)$FAILED_CHANGE"
if [ "$FAILED_COUNT" -gt 0 ] && [ -n "$FAILED_FILES" ]; then
    if [ -n "$FAILED_LINES" ]; then
        echo "$FAILED_FILES" | head -n "$FAILED_LINES" | while read -r line; do
            FILE=$(echo "$line" | awk '{print $NF}')
            SIZE=$(echo "$line" | awk '{print $3}' | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')
            DATE=$(echo "$line" | awk '{print $1, $2}')
            echo -e "\t  $FILE ($SIZE bytes, $DATE)"
        done
        if [ "$FAILED_COUNT" -gt "$FAILED_LINES" ]; then
            REMAINING=$((FAILED_COUNT - FAILED_LINES))
            echo -e "\t  ... and $REMAINING more"
        fi
    else
        echo "$FAILED_FILES" | while read -r line; do
            FILE=$(echo "$line" | awk '{print $NF}')
            SIZE=$(echo "$line" | awk '{print $3}' | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')
            DATE=$(echo "$line" | awk '{print $1, $2}')
            echo -e "\t  $FILE ($SIZE bytes, $DATE)"
        done
    fi
fi

# Result folder - count only
RESULT_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}/result/" --recursive 2>/dev/null | grep -c "\.pdf$" || echo "0")
RESULT_COUNT=$(echo "$RESULT_COUNT" | sed 's/^0*//' | tr -d '[:space:]')
RESULT_COUNT=${RESULT_COUNT:-0}
RESULT_CHANGE=$(format_change $RESULT_COUNT $PREV_RESULT_COUNT "result")
echo "  result/ : $RESULT_COUNT PDFs (completed)$RESULT_CHANGE"

# Record history for throughput calculations
append_history

echo "=== Rate Limit Status ==="

# Get in-flight count from DynamoDB
IN_FLIGHT=$(aws dynamodb get-item \
    --table-name "adobe-api-in-flight-tracker" \
    --key '{"counter_id": {"S": "adobe_api_in_flight"}}' \
    --query 'Item.in_flight.N' \
    --output text 2>/dev/null || echo "0")
echo "  In-flight Adobe API calls: $IN_FLIGHT"

# Check for global backoff
BACKOFF=$(aws dynamodb get-item \
    --table-name "adobe-api-in-flight-tracker" \
    --key '{"counter_id": {"S": "global_backoff_until"}}' \
    --query 'Item.backoff_until.N' \
    --output text 2>/dev/null || echo "None")

if [ "$BACKOFF" != "None" ] && [ "$BACKOFF" != "" ]; then
    NOW=$(date +%s)
    REMAINING=$((BACKOFF - NOW))
    if [ $REMAINING -gt 0 ]; then
        echo "  Global backoff: ${REMAINING}s remaining"
    else
        echo "  Global backoff: None"
    fi
else
    echo "  Global backoff: None"
fi

echo "=== Step Function Status ==="

# Get state machine ARN (find it dynamically)
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?contains(name, 'PdfAccessibilityRemediationWorkflow')].stateMachineArn" \
    --output text 2>/dev/null | head -1)

if [ -n "$STATE_MACHINE_ARN" ]; then
    RUNNING=$(aws stepfunctions list-executions \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --status-filter RUNNING \
        --max-results 100 \
        --query 'length(executions)' \
        --output text 2>/dev/null || echo "0")
    echo "  Running executions: $RUNNING"
else
    echo "  Could not find state machine"
fi

echo "=== Summary ==="
# PDFs still processing = pdf_count - result_count (those in pdf/ but not yet in result/)
STILL_PROCESSING=$((${PDF_COUNT:-0} - ${RESULT_COUNT:-0}))
# Ensure we don't go negative if result > pdf (edge case)
if [ "$STILL_PROCESSING" -lt 0 ]; then
    STILL_PROCESSING=0
fi
TOTAL_PENDING=$((${QUEUE_COUNT:-0} + ${RETRY_COUNT:-0} + ${STILL_PROCESSING}))
PENDING_CHANGE=$(format_change $TOTAL_PENDING $PREV_TOTAL_PENDING "pending")
echo "  Total pending: $TOTAL_PENDING$PENDING_CHANGE"
echo "    (queue: ${QUEUE_COUNT:-0}, processing: ${STILL_PROCESSING})"
echo "  Completed: ${RESULT_COUNT:-0}$RESULT_CHANGE"
if [ "${FAILED_COUNT:-0}" -gt 0 ]; then
    echo "  ⚠️  Failed: ${FAILED_COUNT:-0} (review failed/ folder)"
fi

# Calculate throughput if we have previous data
if [ -n "$PREV_TIMESTAMP" ] && [ "$PREV_RESULT_COUNT" != "" ]; then
    COMPLETED_DIFF=$((RESULT_COUNT - PREV_RESULT_COUNT))
    if [ $COMPLETED_DIFF -gt 0 ]; then
        echo "  📈 Processed since last check: $COMPLETED_DIFF PDFs"
    fi
fi

echo ""
echo "=== Throughput Averages ==="

HOUR_DATA=$(calc_throughput 3600)
HOUR_PROCESSED=$(echo "$HOUR_DATA" | cut -d',' -f1)
HOUR_SECONDS=$(echo "$HOUR_DATA" | cut -d',' -f2)
if [ "$HOUR_SECONDS" -gt 0 ] && [ "$HOUR_PROCESSED" -gt 0 ]; then
    HOUR_RATE=$(awk "BEGIN {printf \"%.1f\", ($HOUR_PROCESSED / $HOUR_SECONDS) * 3600}")
    HOUR_MINS=$((HOUR_SECONDS / 60))
    echo "  Per hour:  $HOUR_RATE PDFs/hr (based on last ${HOUR_MINS}m)"
else
    echo "  Per hour:  -- (collecting data)"
fi

DAY_DATA=$(calc_throughput 86400)
DAY_PROCESSED=$(echo "$DAY_DATA" | cut -d',' -f1)
DAY_SECONDS=$(echo "$DAY_DATA" | cut -d',' -f2)
if [ "$DAY_SECONDS" -gt 0 ] && [ "$DAY_PROCESSED" -gt 0 ]; then
    DAY_RATE=$(awk "BEGIN {printf \"%.1f\", ($DAY_PROCESSED / $DAY_SECONDS) * 86400}")
    DAY_HOURS=$((DAY_SECONDS / 3600))
    echo "  Per day:   $DAY_RATE PDFs/day (based on last ${DAY_HOURS}h, actual: $DAY_PROCESSED)"
else
    echo "  Per day:   -- (collecting data)"
fi

WEEK_DATA=$(calc_throughput 604800)
WEEK_PROCESSED=$(echo "$WEEK_DATA" | cut -d',' -f1)
WEEK_SECONDS=$(echo "$WEEK_DATA" | cut -d',' -f2)
if [ "$WEEK_SECONDS" -gt 0 ] && [ "$WEEK_PROCESSED" -gt 0 ]; then
    WEEK_RATE=$(awk "BEGIN {printf \"%.1f\", ($WEEK_PROCESSED / $WEEK_SECONDS) * 604800}")
    WEEK_DAYS=$(awk "BEGIN {printf \"%.1f\", $WEEK_SECONDS / 86400}")
    echo "  Per week:  $WEEK_RATE PDFs/wk (based on last ${WEEK_DAYS}d, actual: $WEEK_PROCESSED)"
else
    echo "  Per week:  -- (collecting data)"
fi

# Estimate time to complete queue
if [ "$HOUR_SECONDS" -gt 0 ] && [ "$HOUR_PROCESSED" -gt 0 ] && [ "$TOTAL_PENDING" -gt 0 ]; then
    HOURLY_RATE=$(awk "BEGIN {printf \"%.2f\", ($HOUR_PROCESSED / $HOUR_SECONDS) * 3600}")
    if [ "$(echo "$HOURLY_RATE > 0" | bc)" -eq 1 ]; then
        HOURS_REMAINING=$(awk "BEGIN {printf \"%.1f\", $TOTAL_PENDING / $HOURLY_RATE}")
        echo ""
        echo "  ⏱️  Est. time to clear queue: ${HOURS_REMAINING} hours"
    fi
fi

# Deadline projection (April 26, 2026)
echo ""
echo "=== Deadline Projection (04/26/2026) ==="
TARGET_TOTAL=250000
DEADLINE_DATE="2026-04-26"
TODAY=$(date +%s)
DEADLINE=$(date -d "$DEADLINE_DATE" +%s)
DAYS_REMAINING=$(( (DEADLINE - TODAY) / 86400 ))
REMAINING_TO_PROCESS=$((TARGET_TOTAL - RESULT_COUNT))

echo "  Target: ${TARGET_TOTAL} PDFs"
echo "  Completed: ${RESULT_COUNT} PDFs"
echo "  Remaining: ${REMAINING_TO_PROCESS} PDFs"
echo "  Days until deadline: ${DAYS_REMAINING}"

if [ "$DAY_SECONDS" -gt 0 ] && [ "$DAY_PROCESSED" -gt 0 ]; then
    DAILY_RATE=$(awk "BEGIN {printf \"%.1f\", ($DAY_PROCESSED / $DAY_SECONDS) * 86400}")
    PROJECTED_COMPLETION=$(awk "BEGIN {printf \"%.0f\", $DAILY_RATE * $DAYS_REMAINING}")
    REQUIRED_DAILY=$(awk "BEGIN {printf \"%.1f\", $REMAINING_TO_PROCESS / $DAYS_REMAINING}")
    
    echo "  Current rate: ${DAILY_RATE} PDFs/day"
    echo "  Required rate: ${REQUIRED_DAILY} PDFs/day"
    echo "  Projected completion: ${PROJECTED_COMPLETION} PDFs by deadline"
    
    # Calculate projected finish date based on current rate
    if [ "$(echo "$DAILY_RATE > 0" | bc)" -eq 1 ]; then
        DAYS_TO_FINISH=$(awk "BEGIN {printf \"%.0f\", $REMAINING_TO_PROCESS / $DAILY_RATE}")
        FINISH_DATE=$(date -d "+${DAYS_TO_FINISH} days" +"%Y-%m-%d")
        echo "  📅 Projected finish date: ${FINISH_DATE} (in ${DAYS_TO_FINISH} days)"
    fi
    
    if [ "$(echo "$PROJECTED_COMPLETION >= $REMAINING_TO_PROCESS" | bc)" -eq 1 ]; then
        BUFFER=$(awk "BEGIN {printf \"%.0f\", $PROJECTED_COMPLETION - $REMAINING_TO_PROCESS}")
        echo "  ✅ ON TRACK (+${BUFFER} buffer)"
    else
        SHORTFALL=$(awk "BEGIN {printf \"%.0f\", $REMAINING_TO_PROCESS - $PROJECTED_COMPLETION}")
        RATE_INCREASE=$(awk "BEGIN {printf \"%.1f\", ($REQUIRED_DAILY / $DAILY_RATE - 1) * 100}")
        echo "  ⚠️  BEHIND SCHEDULE (shortfall: ${SHORTFALL} PDFs)"
        echo "      Need ${RATE_INCREASE}% rate increase to meet deadline"
    fi
else
    echo "  -- (collecting throughput data)"
fi

# Save current state for next run
save_state

sleep 2m
done
