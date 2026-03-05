# List running executions
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:443915991206:stateMachine:PdfAccessibilityRemediationWorkflow5DEE8455-CkyOV36QLa43 \
  --status-filter RUNNING \
  --query 'executions[].executionArn' \
  --output text
