from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_iam as iam,
)
from constructs import Construct


class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get bucket names from context (pass via -c flag or cdk.context.json)
        source_bucket_name = self.node.try_get_context("source_bucket")
        destination_bucket_name = self.node.try_get_context("destination_bucket")

        if not source_bucket_name or not destination_bucket_name:
            raise ValueError(
                "Must provide source_bucket and destination_bucket context values. "
                "Use: cdk deploy -c source_bucket=<name> -c destination_bucket=<name>"
            )

        # Reference existing buckets
        source_bucket = s3.Bucket.from_bucket_name(
            self, "SourceBucket", source_bucket_name
        )
        destination_bucket = s3.Bucket.from_bucket_name(
            self, "DestinationBucket", destination_bucket_name
        )

        # S3 PDF Copier Lambda
        pdf_copier_lambda = _lambda.Function(
            self,
            "S3PdfCopierLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="main.handler",
            code=_lambda.Code.from_asset("lambda/s3-pdf-copier"),
            memory_size=128,
            timeout=Duration.seconds(30),
            environment={
                "DESTINATION_BUCKET": destination_bucket_name,
            },
        )

        # Grant permissions
        source_bucket.grant_read(pdf_copier_lambda)
        destination_bucket.grant_write(pdf_copier_lambda)

        # Add S3 event notification for /result prefix
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(pdf_copier_lambda),
            s3.NotificationKeyFilter(prefix="result/"),
        )
