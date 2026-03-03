aws iam create-policy \
  --policy-name PDFDashboardViewerPolicy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudWatchDashboardWidgetInvoke",
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": [
                "arn:aws:lambda:*:*:function:rate-limit-widget",
                "arn:aws:lambda:*:*:function:in-flight-files-widget",
                "arn:aws:lambda:*:*:function:success-rate-widget"
            ]
        },
        {
            "Sid": "CloudWatchDashboardReadOnly",
            "Effect": "Allow",
            "Action": [
                "cloudwatch:GetDashboard",
                "cloudwatch:ListDashboards",
                "cloudwatch:GetMetricData",
                "cloudwatch:GetMetricStatistics",
                "cloudwatch:DescribeAlarms"
            ],
            "Resource": "*"
        }
    ]
}'
