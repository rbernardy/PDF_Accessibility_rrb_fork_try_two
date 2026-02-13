"""
PDF Processing Report Generator Lambda

This Lambda function generates a CSV report of all processed PDF files.
It retrieves file metadata and accessibility report data from S3.

The report includes:
- File path, name, size, page count
- Before and after remediation accessibility metrics
"""

import os
import io
import csv
import json
import boto3
from datetime import datetime
from typing import Dict, List, Optional, Any
from pypdf import PdfReader


# Initialize S3 client
s3_client = boto3.client('s3')


def get_pdf_page_count(bucket: str, key: str) -> int:
    """
    Get the number of pages in a PDF file from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        Number of pages in the PDF, or 0 if unable to read
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        pdf_bytes = response['Body'].read()
        reader = PdfReader(io.BytesIO(pdf_bytes))
        return len(reader.pages)
    except Exception as e:
        print(f"Error getting page count for {key}: {e}")
        return 0


def get_file_size(bucket: str, key: str) -> int:
    """
    Get the size of a file in S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        File size in bytes, or 0 if unable to get
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        print(f"Error getting file size for {key}: {e}")
        return 0


def list_result_pdfs(bucket: str) -> List[Dict[str, Any]]:
    """
    List all PDF files in the result folder.
    
    Args:
        bucket: S3 bucket name
        
    Returns:
        List of dictionaries with file info (key, size, last_modified)
    """
    pdf_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix='result/'):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith('.pdf'):
                    pdf_files.append({
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    })
    except Exception as e:
        print(f"Error listing PDFs in result folder: {e}")
    
    return pdf_files


def extract_folder_path_from_result_key(result_key: str) -> str:
    """
    Extract the folder path from a result key.
    
    Example: result/folder1/folder2/COMPLIANT_file.pdf -> folder1/folder2
    
    Args:
        result_key: The S3 key from the result folder
        
    Returns:
        The folder path without 'result/' prefix and filename
    """
    # Remove 'result/' prefix
    path_without_prefix = result_key.replace('result/', '', 1)
    # Remove the filename
    parts = path_without_prefix.rsplit('/', 1)
    if len(parts) > 1:
        return parts[0]
    return ''


def extract_original_filename(result_key: str) -> str:
    """
    Extract the original filename from a result key.
    
    Example: result/folder/COMPLIANT_file.pdf -> file.pdf
    
    Args:
        result_key: The S3 key from the result folder
        
    Returns:
        The original filename without COMPLIANT_ prefix
    """
    filename = os.path.basename(result_key)
    if filename.startswith('COMPLIANT_'):
        return filename.replace('COMPLIANT_', '', 1)
    return filename


def get_accessibility_report_path(folder_path: str, original_filename: str, report_type: str) -> str:
    """
    Construct the S3 path for an accessibility report.
    
    Args:
        folder_path: The folder path (e.g., 'folder1/folder2')
        original_filename: The original PDF filename without COMPLIANT_ prefix
        report_type: Either 'before' or 'after'
        
    Returns:
        The S3 key for the accessibility report
    """
    filename_without_ext = os.path.splitext(original_filename)[0]
    folder_prefix = f"{folder_path}/" if folder_path else ""
    
    if report_type == 'before':
        return f"temp/{folder_prefix}{filename_without_ext}/accessability-report/{filename_without_ext}_accessibility_report_before_remidiation.json"
    else:
        return f"temp/{folder_prefix}{filename_without_ext}/accessability-report/COMPLIANT_{filename_without_ext}_accessibility_report_after_remidiation.json"


def get_error_report_path(folder_path: str, original_filename: str) -> str:
    """
    Construct the S3 path for a pre-remediation error report.
    
    Args:
        folder_path: The folder path (e.g., 'folder1/folder2')
        original_filename: The original PDF filename without COMPLIANT_ prefix
        
    Returns:
        The S3 key for the error report
    """
    filename_without_ext = os.path.splitext(original_filename)[0]
    folder_prefix = f"{folder_path}/" if folder_path else ""
    return f"temp/{folder_prefix}{filename_without_ext}/accessability-report/{filename_without_ext}_pre_remediation_ERROR.json"


def load_json_from_s3(bucket: str, key: str) -> Optional[Dict]:
    """
    Load a JSON file from S3.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        Parsed JSON as dictionary, or None if not found/error
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except s3_client.exceptions.NoSuchKey:
        print(f"JSON file not found: {key}")
        return None
    except Exception as e:
        print(f"Error loading JSON from {key}: {e}")
        return None


def flatten_json(data: Dict, prefix: str = '') -> Dict[str, Any]:
    """
    Flatten a nested JSON structure into a flat dictionary.
    
    Args:
        data: The JSON data to flatten
        prefix: Prefix for keys (used in recursion)
        
    Returns:
        Flattened dictionary with dot-notation keys
    """
    items = {}
    
    if not isinstance(data, dict):
        return {prefix: data} if prefix else {}
    
    for key, value in data.items():
        # Convert key to column-friendly format (spaces to dashes)
        clean_key = str(key).replace(' ', '-').replace('_', '-')
        new_key = f"{prefix}-{clean_key}" if prefix else clean_key
        
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key))
        elif isinstance(value, list):
            # For lists, store the count and optionally the items
            items[f"{new_key}-count"] = len(value)
            # Store first few items as string if they're simple values
            if value and not isinstance(value[0], (dict, list)):
                items[f"{new_key}-values"] = '; '.join(str(v) for v in value[:5])
        else:
            items[new_key] = value
    
    return items


def build_report_row(bucket: str, pdf_info: Dict) -> Dict[str, Any]:
    """
    Build a single row of the report for a PDF file.
    
    Args:
        bucket: S3 bucket name
        pdf_info: Dictionary with PDF file info
        
    Returns:
        Dictionary representing a row in the CSV report
    """
    result_key = pdf_info['key']
    folder_path = extract_folder_path_from_result_key(result_key)
    original_filename = extract_original_filename(result_key)
    
    # Start with basic file info
    row = {
        'file-path': result_key,
        'file-name': os.path.basename(result_key),
        'original-filename': original_filename,
        'folder-path': folder_path,
        'file-size-bytes': pdf_info['size'],
        'last-modified': pdf_info['last_modified'],
        'page-count': get_pdf_page_count(bucket, result_key)
    }
    
    # Load before remediation report
    before_report_key = get_accessibility_report_path(folder_path, original_filename, 'before')
    before_data = load_json_from_s3(bucket, before_report_key)
    if before_data:
        row['before-report-found'] = True
        row['before-report-error'] = False
        flattened_before = flatten_json(before_data, 'before')
        row.update(flattened_before)
    else:
        row['before-report-found'] = False
        # Check if there's an error report
        error_report_key = get_error_report_path(folder_path, original_filename)
        error_data = load_json_from_s3(bucket, error_report_key)
        if error_data:
            row['before-report-error'] = True
            row['before-error-type'] = error_data.get('error_type', 'Unknown')
            row['before-error-message'] = error_data.get('error_message', 'Unknown error')
            row['before-error-timestamp'] = error_data.get('timestamp', '')
        else:
            row['before-report-error'] = False
            row['before-error-type'] = 'MissingReport'
            row['before-error-message'] = 'No before report or error log found'
    
    # Load after remediation report
    after_report_key = get_accessibility_report_path(folder_path, original_filename, 'after')
    after_data = load_json_from_s3(bucket, after_report_key)
    if after_data:
        row['after-report-found'] = True
        flattened_after = flatten_json(after_data, 'after')
        row.update(flattened_after)
    else:
        row['after-report-found'] = False
    
    return row


def collect_all_columns(rows: List[Dict]) -> List[str]:
    """
    Collect all unique column names from all rows.
    
    Args:
        rows: List of row dictionaries
        
    Returns:
        Sorted list of all unique column names
    """
    all_columns = set()
    for row in rows:
        all_columns.update(row.keys())
    
    # Sort columns with basic info first, then status flags, then before, then after
    basic_cols = ['file-path', 'file-name', 'original-filename', 'folder-path', 
                  'file-size-bytes', 'last-modified', 'page-count']
    status_cols = ['before-report-found', 'before-report-error', 'before-error-type', 
                   'before-error-message', 'before-error-timestamp', 'after-report-found']
    before_cols = sorted([c for c in all_columns if c.startswith('before') and c not in status_cols])
    after_cols = sorted([c for c in all_columns if c.startswith('after') and c not in status_cols])
    other_cols = sorted([c for c in all_columns if c not in basic_cols and c not in status_cols
                        and not c.startswith('before') and not c.startswith('after')])
    
    # Filter to only include columns that exist
    status_cols = [c for c in status_cols if c in all_columns]
    
    return basic_cols + status_cols + other_cols + before_cols + after_cols


def generate_csv_content(rows: List[Dict], columns: List[str]) -> str:
    """
    Generate CSV content from rows and columns.
    
    Args:
        rows: List of row dictionaries
        columns: List of column names
        
    Returns:
        CSV content as string
    """
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
    writer.writeheader()
    
    for row in rows:
        writer.writerow(row)
    
    return output.getvalue()


def save_csv_to_s3(bucket: str, csv_content: str) -> str:
    """
    Save CSV content to S3.
    
    Args:
        bucket: S3 bucket name
        csv_content: CSV content as string
        
    Returns:
        S3 key where the CSV was saved
    """
    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    key = f"reports/pdf-processing-report-{timestamp}.csv"
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_content.encode('utf-8'),
        ContentType='text/csv'
    )
    
    print(f"CSV report saved to s3://{bucket}/{key}")
    return key


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Lambda handler for generating PDF processing reports.
    
    Args:
        event: Lambda event (can contain 'bucket' override)
        context: Lambda context
        
    Returns:
        Response with status and report location
    """
    # Get bucket name from event or environment
    bucket = event.get('bucket') or os.environ.get('BUCKET_NAME')
    
    if not bucket:
        return {
            'statusCode': 400,
            'body': 'Missing bucket name. Provide in event or BUCKET_NAME env var.'
        }
    
    print(f"Generating PDF processing report for bucket: {bucket}")
    
    # List all PDFs in result folder
    pdf_files = list_result_pdfs(bucket)
    print(f"Found {len(pdf_files)} PDF files in result folder")
    
    if not pdf_files:
        return {
            'statusCode': 200,
            'body': 'No PDF files found in result folder.'
        }
    
    # Build report rows
    rows = []
    for pdf_info in pdf_files:
        print(f"Processing: {pdf_info['key']}")
        row = build_report_row(bucket, pdf_info)
        rows.append(row)
    
    # Collect all columns and generate CSV
    columns = collect_all_columns(rows)
    csv_content = generate_csv_content(rows, columns)
    
    # Save to S3
    report_key = save_csv_to_s3(bucket, csv_content)
    
    return {
        'statusCode': 200,
        'body': {
            'message': f'Report generated successfully with {len(rows)} files',
            'report_location': f's3://{bucket}/{report_key}',
            'files_processed': len(rows)
        }
    }
