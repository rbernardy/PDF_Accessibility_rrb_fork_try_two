#!/bin/bash
# Resume queue processing after a pause
#
# Usage: ./bin/queue-resume.sh
#
# This sets /pdf-processing/queue-enabled to "true"
# The queue processor Lambda will resume moving files from queue/ to pdf/

echo "Resuming queue processing..."

aws ssm put-parameter \
    --name "/pdf-processing/queue-enabled" \
    --value "true" \
    --type String \
    --overwrite

echo ""
echo "Queue processing is now ENABLED."
echo "Files will be moved from queue/ to pdf/ every 2 minutes."
