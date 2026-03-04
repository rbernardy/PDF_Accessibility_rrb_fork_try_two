"""
Failure Analysis Report Generator Lambda

Generates an Excel spreadsheet from PDF failure analysis data stored in DynamoDB.
Scheduled to run daily at 11:30 PM EST via EventBridge.
"""

import json
import os
import logging
import boto3
from datetime import datetime, timezone, timedelta
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

ANALYSIS_TABLE = os.environ.get('ANALYSIS_TABLE', '')
REPORT_BUCKET = os.environ.get('REPORT_BUCKET', '')


def scan_all_items(table):
    """Scan all items from DynamoDB table with pagination."""
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
    
    return items


def create_excel_report(items: list) -> bytes:
    """Create an Excel spreadsheet from the analysis data."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Failure Analysis"
    
    # Define headers
    headers = [
        'Filename',
        'S3 Key',
        'Analysis Timestamp',
        'API Type',
        'File Size (MB)',
        'Page Count',
        'Image Count',
        'Font Count',
        'Encrypted',
        'PDF Version',
        'Likely Cause',
        'Issue Count',
        'Original Error'
    ]
    
    # Style definitions
    header_font = Font(bold=True, color='FFFFFF')
    header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
    header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Write headers
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
        cell.border = thin_border
    
    # Write data rows
    for row_num, item in enumerate(items, 2):
        row_data = [
            item.get('filename', ''),
            item.get('s3_key', ''),
            item.get('analysis_timestamp', ''),
            item.get('api_type', ''),
            item.get('file_size_mb', ''),
            item.get('page_count', 0),
            item.get('image_count', 0),
            item.get('font_count', 0),
            'Yes' if item.get('has_encryption') else 'No',
            item.get('pdf_version', ''),
            item.get('likely_cause', ''),
            item.get('issue_count', 0),
            item.get('original_error', '')[:500]  # Truncate long errors
        ]
        
        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_num, column=col, value=value)
            cell.border = thin_border
            cell.alignment = Alignment(vertical='top', wrap_text=True)
    
    # Auto-adjust column widths
    column_widths = [30, 50, 25, 12, 12, 12, 12, 12, 10, 12, 50, 12, 60]
    for col, width in enumerate(column_widths, 1):
        ws.column_dimensions[get_column_letter(col)].width = width
    
    # Freeze header row
    ws.freeze_panes = 'A2'
    
    # Save to bytes
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer.getvalue()


def handler(event, context):
    """Generate Excel report from failure analysis data."""
    logger.info(f"Received event: {json.dumps(event)}")
    
    if not ANALYSIS_TABLE:
        logger.error("ANALYSIS_TABLE not configured")
        return {'statusCode': 500, 'body': 'ANALYSIS_TABLE not configured'}
    
    if not REPORT_BUCKET:
        logger.error("REPORT_BUCKET not configured")
        return {'statusCode': 500, 'body': 'REPORT_BUCKET not configured'}
    
    # Scan all items from DynamoDB
    table = dynamodb.Table(ANALYSIS_TABLE)
    logger.info(f"Scanning table: {ANALYSIS_TABLE}")
    items = scan_all_items(table)
    logger.info(f"Found {len(items)} analysis records")
    
    if not items:
        logger.info("No analysis data found, skipping report generation")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No analysis data found'})
        }
    
    # Sort by timestamp descending
    items.sort(key=lambda x: x.get('analysis_timestamp', ''), reverse=True)
    
    # Generate Excel report
    excel_bytes = create_excel_report(items)
    
    # Generate filename with timestamp (US Eastern time)
    eastern_offset = timedelta(hours=-5)
    now_eastern = datetime.now(timezone.utc) + eastern_offset
    timestamp_str = now_eastern.strftime('%Y%m%d_%H%M%S')
    report_key = f"reports/failure_analysis_summary/failure_analysis_report_{timestamp_str}.xlsx"
    
    # Upload to S3
    s3.put_object(
        Bucket=REPORT_BUCKET,
        Key=report_key,
        Body=excel_bytes,
        ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    logger.info(f"Saved report to s3://{REPORT_BUCKET}/{report_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Report generated successfully',
            'record_count': len(items),
            'report_location': f"s3://{REPORT_BUCKET}/{report_key}"
        })
    }
