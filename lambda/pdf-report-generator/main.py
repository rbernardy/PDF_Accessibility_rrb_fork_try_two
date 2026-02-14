"""
PDF Processing Report Generator Lambda

This Lambda function generates an Excel report of all processed PDF files.
It retrieves file metadata and accessibility report data from S3.

The report includes:
- File path, name, size, page count
- Before and after remediation accessibility metrics
"""

import os
import io
import json
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Any
from pypdf import PdfReader
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter


# Initialize S3 client
s3_client = boto3.client('s3')

# Version marker to force Lambda updates
VERSION = "2.0.0-excel"


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


def flatten_json(data: Any, prefix: str = '') -> Dict[str, Any]:
    """
    Flatten a nested JSON structure into a flat dictionary.
    
    Handles the Adobe accessibility report structure:
    - summary array -> before-summary-description, before-summary-needs_manual_check, etc.
    - Detailed Report -> Document -> before-detailed_report-document-rule, etc.
    
    Args:
        data: The JSON data to flatten
        prefix: Prefix for keys (e.g., 'before' or 'after')
        
    Returns:
        Flattened dictionary with hierarchical column names
    """
    items = {}
    
    if data is None:
        return items
    
    if not isinstance(data, (dict, list)):
        # Simple value
        if prefix:
            items[prefix] = data
        return items
    
    if isinstance(data, list):
        # Handle arrays
        if not data:
            items[f"{prefix}-count"] = 0
            return items
        
        # Check if it's an array of objects (like summary array or Detailed Report)
        if isinstance(data[0], dict):
            # Flatten each object in the array
            # For arrays like summary, each item has description, status, etc.
            for i, item in enumerate(data):
                for key, value in item.items():
                    # Create column name: prefix-key (e.g., before-summary-description)
                    # For multiple items with same key, we'll use the first one or concatenate
                    col_name = f"{prefix}-{normalize_key(key)}"
                    
                    if isinstance(value, (dict, list)):
                        # Recursively flatten nested structures
                        nested = flatten_json(value, col_name)
                        items.update(nested)
                    else:
                        # For duplicate keys across array items, append index or concatenate
                        if col_name in items:
                            # Append with index for subsequent items
                            items[f"{col_name}-{i}"] = value
                        else:
                            items[col_name] = value
        else:
            # Array of simple values
            items[f"{prefix}-count"] = len(data)
            items[f"{prefix}-values"] = '; '.join(str(v) for v in data[:10])
        
        return items
    
    # Handle dictionaries
    for key, value in data.items():
        col_name = f"{prefix}-{normalize_key(key)}" if prefix else normalize_key(key)
        
        if isinstance(value, dict):
            # Recursively flatten nested dicts
            nested = flatten_json(value, col_name)
            items.update(nested)
        elif isinstance(value, list):
            # Handle arrays
            nested = flatten_json(value, col_name)
            items.update(nested)
        else:
            # Simple value
            items[col_name] = value
    
    return items


def normalize_key(key: str) -> str:
    """
    Normalize a JSON key to a column-friendly format.
    
    - Spaces become underscores
    - Convert to lowercase
    - Keep underscores as-is
    
    Args:
        key: The original key name
        
    Returns:
        Normalized key name
    """
    # Replace spaces with underscores, convert to lowercase
    normalized = str(key).replace(' ', '_').lower()
    return normalized


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
    
    Orders columns as:
    1. Basic file info columns
    2. Status columns (report found, errors)
    3. All 'before' columns (in order they appear in JSON)
    4. All 'after' columns (in order they appear in JSON)
    
    Args:
        rows: List of row dictionaries
        
    Returns:
        Ordered list of all unique column names
    """
    # Use a list to preserve order of first appearance
    all_columns_ordered = []
    seen = set()
    
    for row in rows:
        for key in row.keys():
            if key not in seen:
                all_columns_ordered.append(key)
                seen.add(key)
    
    # Define column groups
    basic_cols = ['file-path', 'file-name', 'original-filename', 'folder-path', 
                  'file-size-bytes', 'last-modified', 'page-count']
    
    # Status columns - these come right after basic info
    status_cols = ['before-report-found', 'before-report-error', 'before-error-type', 
                   'before-error-message', 'before-error-timestamp', 'after-report-found']
    
    # Separate before and after columns (excluding status cols) - preserve order
    before_cols = [c for c in all_columns_ordered 
                   if c.startswith('before') and c not in status_cols]
    after_cols = [c for c in all_columns_ordered 
                  if c.startswith('after') and c not in status_cols]
    
    # Any other columns that don't fit the above categories - preserve order
    other_cols = [c for c in all_columns_ordered 
                  if c not in basic_cols 
                  and c not in status_cols
                  and not c.startswith('before') 
                  and not c.startswith('after')]
    
    # Filter status_cols to only include those that exist
    status_cols = [c for c in status_cols if c in seen]
    # Filter basic_cols to only include those that exist
    basic_cols = [c for c in basic_cols if c in seen]
    
    # Final order: basic -> status -> other -> before -> after
    return basic_cols + status_cols + other_cols + before_cols + after_cols


def generate_excel_content(rows: List[Dict], columns: List[str]) -> bytes:
    """
    Generate Excel content from rows and columns with formatting.
    
    Args:
        rows: List of row dictionaries
        columns: List of column names
        
    Returns:
        Excel file content as bytes
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "PDF Processing Report"
    
    # Header styling
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    header_alignment = Alignment(horizontal="center", vertical="top", wrap_text=True)
    
    # Write headers
    for col_idx, column in enumerate(columns, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.value = column
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_alignment
    
    # Write data rows
    for row_idx, row in enumerate(rows, start=2):
        for col_idx, column in enumerate(columns, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            value = row.get(column, '')
            cell.value = value
            # Enable text wrapping for all cells
            cell.alignment = Alignment(vertical="top", wrap_text=True)
    
    # Auto-adjust column widths with reasonable limits
    for col_idx, column in enumerate(columns, start=1):
        # Calculate max width based on column name and content
        max_length = len(str(column))
        
        # Check first 100 rows for content length
        for row_idx in range(2, min(len(rows) + 2, 102)):
            cell_value = ws.cell(row=row_idx, column=col_idx).value
            if cell_value:
                # Limit individual cell length check to avoid extremely wide columns
                cell_length = len(str(cell_value))
                if cell_length > max_length:
                    max_length = min(cell_length, 100)  # Cap at 100 chars
        
        # Set column width with min/max bounds
        adjusted_width = min(max(max_length + 2, 15), 80)  # Min 15, max 80
        ws.column_dimensions[get_column_letter(col_idx)].width = adjusted_width
    
    # Freeze the header row
    ws.freeze_panes = "A2"
    
    # Save to bytes
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.read()


def save_excel_to_s3(bucket: str, excel_content: bytes) -> str:
    """
    Save Excel content to S3.
    
    Args:
        bucket: S3 bucket name
        excel_content: Excel file content as bytes
        
    Returns:
        S3 key where the Excel file was saved
    """
    # Get timezone from environment variable, default to US/Eastern
    tz_name = os.environ.get('TZ', 'US/Eastern')
    try:
        tz = ZoneInfo(tz_name)
        timestamp = datetime.now(tz).strftime('%Y%m%d-%H%M%S')
    except Exception as e:
        print(f"Warning: Could not use timezone {tz_name}, falling back to UTC: {e}")
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    
    key = f"reports/pdf-processing-report-{timestamp}.xlsx"
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=excel_content,
        ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    print(f"Excel report saved to s3://{bucket}/{key} (version: {VERSION})")
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
    
    print(f"Generating PDF processing report for bucket: {bucket} (version: {VERSION})")
    
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
    
    # Collect all columns and generate Excel
    columns = collect_all_columns(rows)
    excel_content = generate_excel_content(rows, columns)
    
    # Save to S3
    report_key = save_excel_to_s3(bucket, excel_content)
    
    return {
        'statusCode': 200,
        'body': {
            'message': f'Report generated successfully with {len(rows)} files',
            'report_location': f's3://{bucket}/{report_key}',
            'files_processed': len(rows)
        }
    }
