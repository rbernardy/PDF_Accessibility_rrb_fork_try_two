aws ssm put-parameter --name "/pdf-processing/queue-batch-size" --value "8" --type String --overwrite

aws ssm put-parameter --name "/pdf-processing/queue-batch-size-low-load" --value "15" --type String --overwrite

aws ssm put-parameter --name "/pdf-processing/queue-max-in-flight" --value "12" --type String --overwrite

aws ssm put-parameter --name "/pdf-processing/queue-max-executions" --value "75" --type String --overwrite
