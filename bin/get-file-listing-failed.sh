aws s3 ls s3://${AWS_PROJECT_S3_BUCKET_NAME}/failed/ --recursive | grep .pdf | nl
