#!/bin/bash

# Usage: get-file-listing-failed-summary.sh [--by-time]
#   --by-time: Sort by newest file timestamp (descending) instead of count

S3_OUTPUT=$(aws s3 ls s3://${AWS_PROJECT_S3_BUCKET_NAME}/failed/ --recursive | grep .pdf)

if [[ "$1" == "--by-time" ]]; then
    # Sort by newest file timestamp per folder (descending)
    echo "$S3_OUTPUT" | \
        awk '{
            # Extract date+time and folder name
            datetime = $1 " " $2
            match($4, /failed\/([^\/]+)\//, arr)
            folder = arr[1]
            if (folder != "") {
                count[folder]++
                if (datetime > newest[folder]) newest[folder] = datetime
            }
        }
        END {
            for (f in count) print newest[f], count[f], f
        }' | sort -rn | awk '{print $3, $4, "files, newest:", $1, $2}'
else
    # Sort by count (descending)
    echo "$S3_OUTPUT" | \
        sed 's|.*failed/\([^/]*\)/.*|\1|' | sort | uniq -c | sort -rn
fi
