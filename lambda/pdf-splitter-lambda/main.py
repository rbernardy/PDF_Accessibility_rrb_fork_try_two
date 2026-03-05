"""
This AWS Lambda function is triggered by an S3 event when a PDF file is uploaded to a specified S3 bucket. 
The function performs the following operations:

1. Downloads the PDF file from S3.
2. Pre-scans the PDF to detect "risky" characteristics that may cause Adobe API failures.
3. Splits the PDF into chunks of specified page size (adjusted based on risk assessment).
4. Uploads each PDF chunk to a temporary location in the same S3 bucket.
5. Logs the processing status of each chunk and its upload to S3.
6. Starts an AWS Step Functions execution with metadata about the uploaded chunks.

"""
import json
import boto3
import urllib.parse
import io
import os

# Initialize AWS clients
cloudwatch = boto3.client('cloudwatch')
s3_client = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

state_machine_arn = os.environ['STATE_MACHINE_ARN']


def prescan_pdf_for_risk(pdf_content: bytes, filename: str) -> dict:
    """
    Pre-scan a PDF to detect characteristics that may cause Adobe API failures.
    
    This is a lightweight scan using pypdf to identify "risky" PDFs that should
    use more aggressive splitting to maximize success chances.
    
    Risk factors detected:
    - Large images (scanned documents, newspapers)
    - Unusual page dimensions (non-standard sizes)
    - High image-to-text ratio (image-heavy documents)
    - Mixed page sizes within document
    
    Args:
        pdf_content: Raw PDF bytes
        filename: Filename for logging
        
    Returns:
        dict with 'is_risky', 'risk_factors', 'recommended_pages_per_chunk'
    """
    from pypdf import PdfReader
    
    result = {
        'is_risky': False,
        'risk_factors': [],
        'recommended_pages_per_chunk': 90,  # Default
        'page_count': 0,
        'has_large_pages': False,
        'has_mixed_sizes': False,
        'is_image_heavy': False
    }
    
    try:
        reader = PdfReader(io.BytesIO(pdf_content))
        num_pages = len(reader.pages)
        result['page_count'] = num_pages
        
        if num_pages == 0:
            return result
        
        # Analyze page dimensions
        page_sizes = set()
        large_page_count = 0
        total_images = 0
        
        # Standard page sizes in points (72 points = 1 inch)
        # Letter: 612x792, A4: 595x842, Legal: 612x1008
        STANDARD_WIDTHS = {612, 595, 792, 842, 1008}  # Include rotated
        STANDARD_HEIGHTS = {612, 595, 792, 842, 1008}
        LARGE_DIMENSION_THRESHOLD = 1200  # ~16.7 inches - larger than tabloid
        
        for page_num, page in enumerate(reader.pages):
            # Get page dimensions
            mediabox = page.mediabox
            width = float(mediabox.width)
            height = float(mediabox.height)
            
            # Normalize to portrait orientation for comparison
            w, h = (min(width, height), max(width, height))
            page_sizes.add((round(w), round(h)))
            
            # Check for large/unusual dimensions
            if width > LARGE_DIMENSION_THRESHOLD or height > LARGE_DIMENSION_THRESHOLD:
                large_page_count += 1
            
            # Count images on page (rough estimate)
            if '/XObject' in page.get('/Resources', {}):
                xobjects = page['/Resources'].get('/XObject', {})
                if hasattr(xobjects, 'keys'):
                    for obj_name in xobjects.keys():
                        try:
                            xobj = xobjects[obj_name]
                            if xobj.get('/Subtype') == '/Image':
                                total_images += 1
                        except:
                            pass
        
        # Assess risk factors
        
        # 1. Large/unusual page dimensions (newspapers, posters, scanned docs)
        if large_page_count > 0:
            result['has_large_pages'] = True
            result['risk_factors'].append(f'{large_page_count} pages with large dimensions')
            result['is_risky'] = True
        
        # 2. Mixed page sizes (often problematic)
        if len(page_sizes) > 2:
            result['has_mixed_sizes'] = True
            result['risk_factors'].append(f'{len(page_sizes)} different page sizes')
            result['is_risky'] = True
        
        # 3. Image-heavy documents (high image-to-page ratio)
        images_per_page = total_images / num_pages if num_pages > 0 else 0
        if images_per_page >= 1.0 or total_images > num_pages * 0.8:
            result['is_image_heavy'] = True
            result['risk_factors'].append(f'Image-heavy ({total_images} images in {num_pages} pages)')
            result['is_risky'] = True
        
        # 4. File size per page (rough indicator of complexity)
        file_size_mb = len(pdf_content) / (1024 * 1024)
        mb_per_page = file_size_mb / num_pages if num_pages > 0 else 0
        if mb_per_page > 2.0:  # More than 2MB per page is heavy
            result['risk_factors'].append(f'High density ({mb_per_page:.1f} MB/page)')
            result['is_risky'] = True
        
        # Determine recommended chunking based on risk
        if result['is_risky']:
            if num_pages <= 10:
                # Small risky PDFs: per-page splitting
                result['recommended_pages_per_chunk'] = 1
            elif num_pages <= 20:
                # Medium risky PDFs: 2 pages per chunk
                result['recommended_pages_per_chunk'] = 2
            else:
                # Larger risky PDFs: 5 pages per chunk
                result['recommended_pages_per_chunk'] = 5
        
        # Store analysis details for logging
        result['analysis_details'] = {
            'page_sizes_found': list(page_sizes),
            'large_page_count': large_page_count,
            'total_images': total_images,
            'images_per_page': round(images_per_page, 2),
            'file_size_mb': round(file_size_mb, 2),
            'mb_per_page': round(mb_per_page, 2)
        }
        
        # Always log the full pre-scan report
        print(f'Filename - {filename} | PRE-SCAN REPORT:')
        print(f'Filename - {filename} |   - Pages: {num_pages}')
        print(f'Filename - {filename} |   - File size: {file_size_mb:.2f} MB ({mb_per_page:.2f} MB/page)')
        print(f'Filename - {filename} |   - Page sizes found: {len(page_sizes)} unique sizes: {list(page_sizes)[:5]}{"..." if len(page_sizes) > 5 else ""}')
        print(f'Filename - {filename} |   - Large pages (>{LARGE_DIMENSION_THRESHOLD}pt): {large_page_count}')
        print(f'Filename - {filename} |   - Images detected: {total_images} ({images_per_page:.1f} per page)')
        print(f'Filename - {filename} |   - Risk assessment: {"RISKY" if result["is_risky"] else "NORMAL"}')
        if result['risk_factors']:
            print(f'Filename - {filename} |   - Risk factors: {", ".join(result["risk_factors"])}')
        print(f'Filename - {filename} |   - Recommended pages/chunk: {result["recommended_pages_per_chunk"]}')
        
    except Exception as e:
        print(f'Filename - {filename} | PRE-SCAN: Error during analysis: {e}')
        import traceback
        traceback.print_exc()
        # On error, don't change defaults
    
    return result

def log_chunk_created(filename):
    """
    Logs the creation of a PDF chunk.
    
    This function logs the filename and processing status for each chunk and indicates 
    successful upload of the chunk to S3. It also returns an HTTP status code and a message 
    confirming the update of the processing metric.
    
    Parameters:
        filename (str): The name of the file chunk being processed.

    Returns:
        dict: HTTP response with a status code and a message indicating the metric update.
    """
    print(f"File: {filename}, Status: Processing")
    print(f'Filename - {filename} | Uploaded {filename} to S3')
   
    return {
        'statusCode': 200,
        'body': 'Metric status updated to failed.'
    }

def split_pdf_into_pages(source_content, original_key, s3_client, bucket_name, pages_per_chunk, max_chunk_size_mb=95, retry_count=0):
    """
    Splits a PDF file into chunks based on page count AND file size limits.
    
    Adobe API has a 104MB limit, so we use 95MB as a safe threshold.
    If a chunk exceeds the size limit, it's automatically split into smaller pieces.
    
    For retry attempts (retry_count > 0) with small PDFs (≤10 pages), uses per-page
    splitting to maximize chances of success for previously failed files.
    
    Parameters:
        source_content (bytes): The binary content of the PDF file.
        original_key (str): The original S3 key of the PDF file.
        s3_client (boto3.client): The Boto3 S3 client instance for interacting with S3.
        bucket_name (str): The name of the S3 bucket.
        pages_per_chunk (int): The maximum number of pages per chunk.
        max_chunk_size_mb (int): Maximum chunk size in MB (default 95MB, Adobe limit is 104MB).
        retry_count (int): Number of previous retry attempts (0 = first attempt).

    Returns:
        list: A list of dictionaries containing metadata for each uploaded chunk.
    """
    from pypdf import PdfReader, PdfWriter
    
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
    
    reader = PdfReader(io.BytesIO(source_content))
    num_pages = len(reader.pages)
    file_basename = original_key.split('/')[-1].rsplit('.', 1)[0]
    
    # For retry attempts with small PDFs, use per-page splitting
    # This maximizes success chances for previously failed files
    RETRY_PAGE_THRESHOLD = 10
    is_retry = retry_count > 0
    
    if is_retry and num_pages <= RETRY_PAGE_THRESHOLD:
        pages_per_chunk = 1
        print(f'Filename - {file_basename} | RETRY #{retry_count}: Using per-page splitting for {num_pages}-page PDF')
    elif is_retry:
        # For larger retry PDFs, use smaller chunks but not necessarily per-page
        pages_per_chunk = min(pages_per_chunk, 5)
        print(f'Filename - {file_basename} | RETRY #{retry_count}: Using {pages_per_chunk} pages per chunk for {num_pages}-page PDF')
    
    # Preserve the full original path
    original_pdf_key = original_key
    
    # Extract folder path (everything between 'pdf/' and filename)
    key_without_prefix = original_key.replace('pdf/', '', 1)
    folder_path = key_without_prefix.rsplit('/', 1)[0] if '/' in key_without_prefix else ''
    
    chunks = []
    chunk_index = 0
    current_page = 0
    
    while current_page < num_pages:
        chunk_index += 1
        
        # Try to create a chunk with the target page count
        end_page = min(current_page + pages_per_chunk, num_pages)
        
        # Binary search to find the maximum pages that fit within size limit
        chunk_created = False
        while not chunk_created and current_page < end_page:
            output = io.BytesIO()
            writer = PdfWriter()
            
            # Add pages to the current chunk
            for i in range(current_page, end_page):
                writer.add_page(reader.pages[i])
            
            writer.write(output)
            chunk_size = output.tell()
            output.seek(0)
            
            pages_in_chunk = end_page - current_page
            
            if chunk_size <= max_chunk_size_bytes:
                # Chunk is within size limit, upload it
                page_filename = f"{file_basename}_chunk_{chunk_index}.pdf"
                folder_prefix = f"{folder_path}/" if folder_path else ""
                s3_key = f"temp/{folder_prefix}{file_basename}/{page_filename}"
                
                s3_client.upload_fileobj(
                    Fileobj=output,
                    Bucket=bucket_name,
                    Key=s3_key
                )
                
                chunk_size_mb = chunk_size / (1024 * 1024)
                print(f'Filename - {page_filename} | Uploaded to S3 at {s3_key} | Pages: {pages_in_chunk} | Size: {chunk_size_mb:.1f}MB')
                
                chunks.append({
                    "s3_bucket": bucket_name,
                    "s3_key": s3_key,
                    "chunk_key": s3_key,
                    "original_pdf_key": original_pdf_key,
                    "folder_path": folder_path
                })
                
                current_page = end_page
                chunk_created = True
            else:
                # Chunk too large, reduce page count
                if pages_in_chunk <= 1:
                    # Single page exceeds limit - upload anyway and let Adobe handle the error
                    # This is an edge case for extremely large single pages
                    page_filename = f"{file_basename}_chunk_{chunk_index}.pdf"
                    folder_prefix = f"{folder_path}/" if folder_path else ""
                    s3_key = f"temp/{folder_prefix}{file_basename}/{page_filename}"
                    
                    s3_client.upload_fileobj(
                        Fileobj=output,
                        Bucket=bucket_name,
                        Key=s3_key
                    )
                    
                    chunk_size_mb = chunk_size / (1024 * 1024)
                    print(f'WARNING - {page_filename} | Single page exceeds size limit ({chunk_size_mb:.1f}MB) | Uploading anyway')
                    
                    chunks.append({
                        "s3_bucket": bucket_name,
                        "s3_key": s3_key,
                        "chunk_key": s3_key,
                        "original_pdf_key": original_pdf_key,
                        "folder_path": folder_path
                    })
                    
                    current_page = end_page
                    chunk_created = True
                else:
                    # Reduce pages by half and retry
                    reduction = max(1, pages_in_chunk // 2)
                    end_page = current_page + (pages_in_chunk - reduction)
                    chunk_size_mb = chunk_size / (1024 * 1024)
                    print(f'Filename - {file_basename} | Chunk too large ({chunk_size_mb:.1f}MB), reducing from {pages_in_chunk} to {end_page - current_page} pages')

    return chunks


def lambda_handler(event, context):
    """
    AWS Lambda function to handle S3 events and split uploaded PDF files into chunks.

    This function is triggered when a PDF file is uploaded to an S3 bucket. It:
    1. Downloads the PDF from S3
    2. Pre-scans the PDF to detect risky characteristics (large images, unusual dimensions, etc.)
    3. Determines optimal chunk size based on pre-scan results AND retry status
    4. Splits the PDF into chunks and uploads them to S3
    5. Starts a Step Functions execution to process the chunks
    
    Splitting strategy uses the MORE AGGRESSIVE (smaller) of:
    - Pre-scan recommendation: Based on PDF characteristics detected during analysis
    - Retry recommendation: Based on previous failure attempts (via S3 metadata 'retry-count')
    
    This proactive approach avoids wasted processing time on PDFs likely to fail.

    Parameters:
        event (dict): The S3 event that triggered the Lambda function, containing the S3 bucket 
                      and object key information.

    Returns:
        dict: HTTP response indicating the success or failure of the Lambda function execution.
    """
    try:
        
        print("Received event: " + json.dumps(event, indent=2))

        # Access the S3 event structure
        if 'Records' in event and len(event['Records']) > 0:
            s3_record = event['Records'][0]
            bucket_name = s3_record['s3']['bucket']['name']
            pdf_file_key = urllib.parse.unquote_plus(s3_record['s3']['object']['key'])
        else:
            raise ValueError("Event does not contain 'Records'. Check the S3 event structure.")
        file_basename = pdf_file_key.split('/')[-1].rsplit('.', 1)[0]


        s3 = boto3.client('s3')
        stepfunctions = boto3.client('stepfunctions')

        # Check retry count from S3 object metadata (set by pdf-failure-cleanup Lambda)
        retry_count = 0
        try:
            head_response = s3.head_object(Bucket=bucket_name, Key=pdf_file_key)
            metadata = head_response.get('Metadata', {})
            retry_count = int(metadata.get('retry-count', '0'))
            if retry_count > 0:
                print(f'Filename - {pdf_file_key} | RETRY #{retry_count} detected via S3 metadata')
        except Exception as e:
            print(f'Filename - {pdf_file_key} | Could not read metadata: {e}')

        # Get the PDF file from S3
        response = s3.get_object(Bucket=bucket_name, Key=pdf_file_key)
        print(f'Filename - {pdf_file_key} | The response is: {response}')
        pdf_file_content = response['Body'].read()
        
        # Pre-scan PDF for risk factors that may cause Adobe API failures
        # This detects large images, unusual dimensions, image-heavy docs, mixed page sizes
        prescan_result = prescan_pdf_for_risk(pdf_file_content, pdf_file_key)
        prescan_pages_per_chunk = prescan_result['recommended_pages_per_chunk']
        
        # Determine final pages_per_chunk: use the MORE AGGRESSIVE (smaller) of:
        # 1. Pre-scan recommendation (based on PDF characteristics)
        # 2. Retry-based recommendation (based on previous failures)
        default_pages_per_chunk = 90
        
        # Calculate retry-based recommendation
        if retry_count > 0:
            num_pages = prescan_result['page_count']
            if num_pages <= 10:
                retry_pages_per_chunk = 1
            else:
                retry_pages_per_chunk = 5
        else:
            retry_pages_per_chunk = default_pages_per_chunk
        
        # Use the more aggressive (smaller) value
        final_pages_per_chunk = min(prescan_pages_per_chunk, retry_pages_per_chunk)
        
        # Always log the splitting decision
        print(f'Filename - {pdf_file_key} | SPLITTING DECISION:')
        print(f'Filename - {pdf_file_key} |   - Pre-scan recommendation: {prescan_pages_per_chunk} pages/chunk')
        print(f'Filename - {pdf_file_key} |   - Retry recommendation: {retry_pages_per_chunk} pages/chunk (retry_count={retry_count})')
        print(f'Filename - {pdf_file_key} |   - FINAL DECISION: {final_pages_per_chunk} pages/chunk')
        print(f'Filename - {pdf_file_key} |   - Total pages: {prescan_result["page_count"]}')
        print(f'Filename - {pdf_file_key} |   - Expected chunks: ~{max(1, prescan_result["page_count"] // final_pages_per_chunk)}')
  
        # Split the PDF into pages and upload them to S3
        # Uses both page count AND file size (95MB max) limits
        # Adobe API limit is 104MB, we use 95MB for safety margin
        # Note: retry_count=0 passed since we already calculated final_pages_per_chunk above
        chunks = split_pdf_into_pages(pdf_file_content, pdf_file_key, s3, bucket_name, final_pages_per_chunk, max_chunk_size_mb=95, retry_count=0)
        
        # Log splitting result summary
        print(f'Filename - {pdf_file_key} | SPLITTING COMPLETE: Created {len(chunks)} chunk(s) from {prescan_result["page_count"]} pages')
        
        log_chunk_created(file_basename)

        # Trigger Step Function with the list of chunks
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps({
                "chunks": chunks, 
                "s3_bucket": bucket_name,
                "original_pdf_key": pdf_file_key,
                "retry_count": retry_count,
                "prescan": {
                    "is_risky": prescan_result['is_risky'],
                    "risk_factors": prescan_result['risk_factors'],
                    "pages_per_chunk": final_pages_per_chunk
                }
            })
        )
        print(f"Filename - {pdf_file_key} | Step Function started: {response['executionArn']} | retry_count: {retry_count} | risky: {prescan_result['is_risky']} | pages_per_chunk: {final_pages_per_chunk}")

    except KeyError as e:
 
        print(f"File: {file_basename}, Status: Failed in split lambda function")
        print(f"Filename - {pdf_file_key} | KeyError: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: Missing key in event: {str(e)}")
        }
    except ValueError as e:
  
        print(f"File: {file_basename}, Status: Failed in split lambda function")
        print(f"Filename - {pdf_file_key} | ValueError: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
    except Exception as e:

        print(f"File: {file_basename}, Status: Failed in split lambda function")
        print(f"Filename - {pdf_file_key} | Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing event: {str(e)}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Event processed successfully!')
    }
