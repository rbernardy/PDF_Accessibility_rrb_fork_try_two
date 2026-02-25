Summary of Changes
1. Updated 
main.py
:

Added rate limit detection patterns (429, Too Many Requests, throttle, etc.)
New is_rate_limit_failure() function to detect transient failures
New move_pdf_to_retry_folder() function that moves PDFs from pdf/[folder]/file.pdf to retry/[folder]/file.pdf (preserving folder structure)
Modified handler to:
For 429/rate limit failures: Move to retry/ folder instead of deleting
For permanent failures: Delete and create placeholder (existing behavior)
Always clean up temp folder
Track moved_to_retry flag in DynamoDB records
2. Created new lambda/pdf-retry-processor/ Lambda:

main.py - Scheduled Lambda that runs every 5 minutes
requirements.txt - Dependencies
Dockerfile - Container build
Logic:
Checks global backoff (skips if active)
Checks in-flight count (skips if >= 5)
Checks running Step Function executions (skips if >= 10)
Lists files in retry/ folder (FIFO order - oldest first)
Moves 1-3 files back to pdf/ folder when capacity available
The S3 trigger on pdf/ automatically restarts processing
3. Updated app.py:

Added the new pdf-retry-processor Lambda definition
Configured EventBridge schedule (every 5 minutes)
Granted necessary permissions (S3, DynamoDB, Step Functions)
Flow:

PDF fails with 429 → Cleanup Lambda detects rate limit → Moves to retry/[folder]/file.pdf
Every 5 minutes, retry processor checks queue capacity
When capacity available, moves file from retry/ back to pdf/
S3 trigger fires → Processing restarts automatically
Credits used: 11.98
Elapsed time: 9m 35s
