# Verify new email in SES
aws ses verify-email-identity --email-address new-email@usf.edu

# Update the parameter
aws ssm put-parameter --name "/pdf-processing/sender-email" --value "new-email@usf.edu" --overwrite
