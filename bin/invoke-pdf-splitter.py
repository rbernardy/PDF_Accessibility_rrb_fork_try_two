#!/usr/bin/env python3
"""
Invoke PDF Splitter Lambda

Manually triggers the PDF splitter Lambda function for a specific S3 file.
This simulates the S3 event that normally triggers the Lambda when a file
is uploaded to the pdf/ folder.

Usage:
    ./bin/invoke-pdf-splitter.py <s3_path>

Examples:
    ./bin/invoke-pdf-splitter.py s3://my-bucket/pdf/folder/document.pdf
    ./bin/invoke-pdf-splitter.py pdf/folder/document.pdf --bucket my-bucket
    ./bin/invoke-pdf-splitter.py pdf/folder/document.pdf  # Uses default bucket from SSM

Options:
    --bucket        S3 bucket name (optional if using full s3:// path or SSM default)
    --lambda-name   Lambda function name (default: pdf-splitter-lambda)
    --dry-run       Show the event payload without invoking the Lambda
    --async         Invoke asynchronously (don't wait for response)
"""

import argparse
import json
import sys
import boto3
from botocore.exceptions import ClientError


def get_default_bucket():
    """Try to get the default bucket from SSM parameter or environment."""
    ssm = boto3.client('ssm')
    try:
        response = ssm.get_parameter(Name='/pdf-processing/bucket-name')
        return response['Parameter']['Value']
    except ClientError:
        pass
    
    # Try common parameter names
    for param_name in ['/pdf-processing/s3-bucket', '/pdf-remediation/bucket']:
        try:
            response = ssm.get_parameter(Name=param_name)
            return response['Parameter']['Value']
        except ClientError:
            continue
    
    return None


def parse_s3_path(s3_path: str, bucket_override: str = None) -> tuple:
    """
    Parse S3 path into bucket and key.
    
    Accepts:
        s3://bucket-name/path/to/file.pdf
        bucket-name/path/to/file.pdf
        path/to/file.pdf (with --bucket option)
    
    Returns:
        (bucket_name, key)
    """
    if s3_path.startswith('s3://'):
        # Full S3 URI
        path = s3_path[5:]  # Remove 's3://'
        parts = path.split('/', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 path: {s3_path}")
        return parts[0], parts[1]
    elif bucket_override:
        # Key only, bucket provided separately
        return bucket_override, s3_path
    elif '/' in s3_path:
        # Try to detect if first part is bucket name
        parts = s3_path.split('/', 1)
        # If it starts with pdf/, queue/, etc., it's probably just a key
        if parts[0] in ['pdf', 'queue', 'retry', 'temp', 'result', 'failed']:
            # Need bucket from default
            default_bucket = get_default_bucket()
            if default_bucket:
                return default_bucket, s3_path
            else:
                raise ValueError(
                    f"Cannot determine bucket for path: {s3_path}\n"
                    "Please provide full s3:// path or use --bucket option"
                )
        else:
            # Assume first part is bucket
            return parts[0], parts[1]
    else:
        raise ValueError(f"Invalid S3 path: {s3_path}")


def create_s3_event(bucket: str, key: str) -> dict:
    """Create a mock S3 event payload that mimics what S3 sends to Lambda."""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": boto3.session.Session().region_name or "us-east-1",
                "eventTime": "2024-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {
                        "name": bucket,
                        "arn": f"arn:aws:s3:::{bucket}"
                    },
                    "object": {
                        "key": key,
                        "size": 0,
                        "eTag": "manual-invocation"
                    }
                }
            }
        ]
    }


def invoke_lambda(function_name: str, payload: dict, async_invoke: bool = False) -> dict:
    """Invoke the Lambda function with the given payload."""
    lambda_client = boto3.client('lambda')
    
    invocation_type = 'Event' if async_invoke else 'RequestResponse'
    
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType=invocation_type,
        Payload=json.dumps(payload)
    )
    
    if async_invoke:
        return {'StatusCode': response['StatusCode'], 'async': True}
    
    # Read response payload
    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    return {
        'StatusCode': response['StatusCode'],
        'FunctionError': response.get('FunctionError'),
        'Payload': response_payload
    }


def main():
    parser = argparse.ArgumentParser(
        description='Invoke the PDF splitter Lambda for a specific S3 file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s s3://my-bucket/pdf/reports/doc.pdf
  %(prog)s pdf/reports/doc.pdf --bucket my-bucket
  %(prog)s pdf/reports/doc.pdf --dry-run
  %(prog)s s3://my-bucket/pdf/reports/doc.pdf --async
        """
    )
    parser.add_argument('s3_path', help='S3 path to the PDF file (s3://bucket/key or just key)')
    parser.add_argument('--bucket', '-b', help='S3 bucket name (if not in s3_path)')
    parser.add_argument('--lambda-name', '-l', default='pdf-splitter-lambda',
                        help='Lambda function name (default: pdf-splitter-lambda)')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Show event payload without invoking Lambda')
    parser.add_argument('--async', '-a', dest='async_invoke', action='store_true',
                        help='Invoke asynchronously (don\'t wait for response)')
    
    args = parser.parse_args()
    
    # Parse S3 path
    try:
        bucket, key = parse_s3_path(args.s3_path, args.bucket)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Validate key format
    if not key.lower().endswith('.pdf'):
        print(f"Warning: File does not have .pdf extension: {key}", file=sys.stderr)
    
    # Create event payload
    event = create_s3_event(bucket, key)
    
    print(f"Bucket: {bucket}")
    print(f"Key: {key}")
    print(f"Lambda: {args.lambda_name}")
    print()
    
    if args.dry_run:
        print("Event payload (dry-run, not invoking):")
        print(json.dumps(event, indent=2))
        return
    
    # Verify file exists in S3
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        print(f"✓ File exists in S3")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error: File not found: s3://{bucket}/{key}", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"Warning: Could not verify file exists: {e}", file=sys.stderr)
    
    # Invoke Lambda
    print(f"Invoking {args.lambda_name}...")
    print()
    
    try:
        result = invoke_lambda(args.lambda_name, event, args.async_invoke)
        
        if args.async_invoke:
            print(f"✓ Lambda invoked asynchronously (StatusCode: {result['StatusCode']})")
            print("Check CloudWatch logs for results.")
        else:
            print(f"StatusCode: {result['StatusCode']}")
            if result.get('FunctionError'):
                print(f"FunctionError: {result['FunctionError']}")
            print()
            print("Response:")
            print(json.dumps(result['Payload'], indent=2))
            
            if result['StatusCode'] == 200 and not result.get('FunctionError'):
                print()
                print("✓ PDF splitter invoked successfully")
            else:
                print()
                print("✗ Lambda returned an error")
                sys.exit(1)
                
    except ClientError as e:
        print(f"Error invoking Lambda: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
