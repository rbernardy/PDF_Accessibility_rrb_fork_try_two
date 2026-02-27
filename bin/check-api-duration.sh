#!/bin/bash
#
# Query CloudWatch Logs to get Adobe API call duration statistics
# Shows average, min, max, and percentiles for autotag and extract API calls
#
# Usage: ./bin/check-api-duration.sh [hours]
#   hours: How many hours back to query (default: 24)
#

HOURS=${1:-24}
LOG_GROUP="/ecs/pdf-remediation/adobe-autotag"

echo "=== Adobe API Duration Statistics (last ${HOURS}h) ==="
echo ""

# Calculate time range
END_TIME=$(date +%s)000
START_TIME=$(( (END_TIME / 1000 - HOURS * 3600) * 1000 ))

# Query for API durations
QUERY_ID=$(aws logs start-query \
    --log-group-name "$LOG_GROUP" \
    --start-time $START_TIME \
    --end-time $END_TIME \
    --query-string 'fields @message
        | filter @message like /API duration:/
        | parse @message "* | API duration: *s" as prefix, duration
        | parse prefix "* | Adobe * completed" as file_info, api_type
        | stats 
            count() as total_calls,
            avg(duration) as avg_duration,
            min(duration) as min_duration,
            max(duration) as max_duration,
            pct(duration, 50) as p50,
            pct(duration, 90) as p90,
            pct(duration, 99) as p99
            by api_type' \
    --query 'queryId' \
    --output text 2>/dev/null)

if [ -z "$QUERY_ID" ]; then
    echo "Error: Could not start CloudWatch query."
    echo "Make sure the log group exists and you have permissions."
    echo ""
    echo "Note: API duration logging requires a deployment after this update."
    exit 1
fi

echo "Running query..."

# Wait for query to complete
sleep 3

# Get results
RESULTS=$(aws logs get-query-results --query-id "$QUERY_ID" 2>/dev/null)
STATUS=$(echo "$RESULTS" | jq -r '.status')

# Wait longer if still running
WAIT_COUNT=0
while [ "$STATUS" = "Running" ] && [ $WAIT_COUNT -lt 10 ]; do
    sleep 2
    RESULTS=$(aws logs get-query-results --query-id "$QUERY_ID" 2>/dev/null)
    STATUS=$(echo "$RESULTS" | jq -r '.status')
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$STATUS" != "Complete" ]; then
    echo "Query did not complete in time (status: $STATUS)"
    exit 1
fi

# Check if we have results
RESULT_COUNT=$(echo "$RESULTS" | jq '.results | length')

if [ "$RESULT_COUNT" -eq 0 ]; then
    echo "No API duration data found."
    echo ""
    echo "This could mean:"
    echo "  1. No PDFs have been processed in the last ${HOURS}h"
    echo "  2. The duration logging hasn't been deployed yet"
    echo ""
    echo "Deploy the latest code and process some PDFs to collect duration data."
    exit 0
fi

echo ""
echo "Results:"
echo "--------"

# Parse and display results
echo "$RESULTS" | jq -r '.results[] | 
    "API Type: \(.[] | select(.field == "api_type") | .value)
    Total Calls: \(.[] | select(.field == "total_calls") | .value)
    Average: \(.[] | select(.field == "avg_duration") | .value)s
    Min: \(.[] | select(.field == "min_duration") | .value)s
    Max: \(.[] | select(.field == "max_duration") | .value)s
    P50: \(.[] | select(.field == "p50") | .value)s
    P90: \(.[] | select(.field == "p90") | .value)s
    P99: \(.[] | select(.field == "p99") | .value)s
    "'

echo ""
echo "=== Recommendation ==="

# Get the average duration for calculation
AVG_AUTOTAG=$(echo "$RESULTS" | jq -r '.results[] | select(.[] | select(.field == "api_type" and .value == "Autotag")) | .[] | select(.field == "avg_duration") | .value' 2>/dev/null)
AVG_EXTRACT=$(echo "$RESULTS" | jq -r '.results[] | select(.[] | select(.field == "api_type" and .value == "Extract")) | .[] | select(.field == "avg_duration") | .value' 2>/dev/null)

if [ -n "$AVG_AUTOTAG" ] || [ -n "$AVG_EXTRACT" ]; then
    # Use the higher of the two averages
    if [ -n "$AVG_AUTOTAG" ] && [ -n "$AVG_EXTRACT" ]; then
        AVG=$(echo "$AVG_AUTOTAG $AVG_EXTRACT" | awk '{print ($1 > $2) ? $1 : $2}')
    elif [ -n "$AVG_AUTOTAG" ]; then
        AVG=$AVG_AUTOTAG
    else
        AVG=$AVG_EXTRACT
    fi
    
    # Calculate recommended max-in-flight for 200 RPM
    # Formula: max_in_flight = RPM * (avg_duration_seconds / 60)
    RECOMMENDED=$(echo "$AVG" | awk '{printf "%.0f", 200 * ($1 / 60)}')
    
    echo "Based on average API duration of ${AVG}s:"
    echo "  For 200 RPM limit: max-in-flight = $RECOMMENDED"
    echo ""
    echo "To apply:"
    echo "  aws ssm put-parameter --name \"/pdf-processing/adobe-api-max-in-flight\" --value \"$RECOMMENDED\" --type String --overwrite"
fi
