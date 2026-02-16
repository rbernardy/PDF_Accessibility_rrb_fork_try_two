"""
Lambda function to copy PDF files from source S3 bucket to destination bucket.
Triggered by S3 Event Notification on object creation under /result prefix.
"""

import logging
import os
import urllib.parse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def handler(event, context):
    """Process S3 event and copy PDF to destination bucket."""
    record = event["Records"][0]
    source_bucket = record["s3"]["bucket"]["name"]
    source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    logger.info(f"Source bucket: {source_bucket}, key: {source_key}")

    # Validate PDF extension (case-insensitive)
    if not source_key.lower().endswith(".pdf"):
        logger.info(f"Skipping non-PDF file: {source_key}")
        return {"statusCode": 200, "body": "Skipped non-PDF file"}

    # Get destination bucket from environment
    destination_bucket = os.environ["DESTINATION_BUCKET"]

    # Strip /result prefix from key
    if source_key.startswith("result/"):
        destination_key = source_key[len("result"):]
    elif source_key.startswith("/result/"):
        destination_key = source_key[len("/result"):]
    else:
        destination_key = "/" + source_key

    logger.info(f"Destination bucket: {destination_bucket}, key: {destination_key}")

    try:
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=destination_key.lstrip("/"),
        )
        logger.info(f"Successfully copied to {destination_bucket}/{destination_key}")
        return {"statusCode": 200, "body": "Copy successful"}
    except ClientError as e:
        logger.error(f"Failed to copy file: {e}")
        raise
