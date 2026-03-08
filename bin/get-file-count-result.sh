aws s3 ls s3://${AWS_PROJECT_S3_BUCKET_NAME}/result/ --recursive | grep .pdf | wc -l
