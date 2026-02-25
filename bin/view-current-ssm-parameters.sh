aws ssm get-parameters-by-path --path "/pdf-processing/" --query "Parameters[*].[Name,Value]" --output table
