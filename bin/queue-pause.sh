#!/bin/bash
# Pause queue processing (useful during large batch uploads)
#
# Usage: ./bin/queue-pause.sh
#
# This sets /pdf-processing/queue-enabled to "false"
# The queue processor Lambda will skip processing until re-enabled
# Files already in the pdf/ folder will continue processing

echo "Pausing queue processing..."

aws ssm put-parameter \
    --name "/pdf-processing/queue-enabled" \
    --value "false" \
    --type String \
    --overwrite

echo ""
echo "Queue processing is now PAUSED."
echo "Files in queue/ folder will NOT be moved to pdf/ folder."
echo "Files already in pdf/ folder will continue processing."
echo ""
echo "To resume: ./bin/queue-resume.sh"
