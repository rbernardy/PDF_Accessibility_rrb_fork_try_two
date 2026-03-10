aws s3 ls s3://${AWS_PROJECT_S3_BUCKET_NAME}/queue/ --recursive | grep '\.pdf$' | awk -F'/' '{print $2}' | sort | uniq -c | sort -k2 | awk '{print $2, $1}'
