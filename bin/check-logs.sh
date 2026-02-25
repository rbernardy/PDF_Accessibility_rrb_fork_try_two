# 1. Check if there are files in the retry folder
aws s3 ls s3://pdfaccessibility-pdfaccessibilitybucket149b7021e-ljzn29qgmwog/retry/ --recursive | head -50

# 2. Check the cleanup Lambda logs (last 30 minutes)
aws logs filter-log-events \
    --log-group-name "/aws/lambda/pdf-failure-cleanup-handler" \
    --start-time $(date -d '30 minutes ago' +%s)000 \
    --filter-pattern "rate limit" \
    --limit 20

# 3. Check cleanup Lambda for what action it took
aws logs filter-log-events \
    --log-group-name "/aws/lambda/pdf-failure-cleanup-handler" \
    --start-time $(date -d '30 minutes ago' +%s)000 \
    --filter-pattern "action" \
    --limit 30

# 4. Check retry processor Lambda logs
aws logs filter-log-events \
    --log-group-name "/aws/lambda/pdf-retry-processor" \
    --start-time $(date -d '60 minutes ago' +%s)000 \
    --limit 30

# 5. Check what failure reasons are being detected
aws logs filter-log-events \
    --log-group-name "/aws/lambda/pdf-failure-cleanup-handler" \
    --start-time $(date -d '30 minutes ago' +%s)000 \
    --filter-pattern "failure_reason" \
    --limit 20

