# Check Step Function executions - see if any are still RUNNING or failed
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:443915991205stateMachine:PdfAccessibilityRemediationWorkflow \
  --status-filter RUNNING \
  --max-results 10

# Check for FAILED executions around that time
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:443915991205:stateMachine:PdfAccessibilityRemediationWorkflow5DEE8455-CkyOV36QLa43 \
  --status-filter FAILED \
  --max-results 20

