"""
This AWS Lambda function is triggered by an S3 event when a PDF file is uploaded to a specified S3 bucket. 
The function performs the following operations:

1. Downloads the PDF file from S3.
2. Splits the PDF into chunks of specified page size (for example, one page per chunk).
3. Uploads each PDF chunk to a temporary location in the same S3 bucket.
4. Logs the processing status of each chunk and its upload to S3.
5. Starts an AWS Step Functions execution with metadata about the uploaded chunks.

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

def split_pdf_into_pages(source_content, original_key, s3_client, bucket_name, pages_per_chunk, max_chunk_size_mb=95):
    """
    Splits a PDF file into chunks based on page count AND file size limits.
    
    Adobe API has a 104MB limit, so we use 95MB as a safe threshold.
    If a chunk exceeds the size limit, it's automatically split into smaller pieces.
    
    Parameters:
        source_content (bytes): The binary content of the PDF file.
        original_key (str): The original S3 key of the PDF file.
        s3_client (boto3.client): The Boto3 S3 client instance for interacting with S3.
        bucket_name (str): The name of the S3 bucket.
        pages_per_chunk (int): The maximum number of pages per chunk.
        max_chunk_size_mb (int): Maximum chunk size in MB (default 95MB, Adobe limit is 104MB).

    Returns:
        list: A list of dictionaries containing metadata for each uploaded chunk.
    """
    from pypdf import PdfReader, PdfWriter
    
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
    
    reader = PdfReader(io.BytesIO(source_content))
    num_pages = len(reader.pages)
    file_basename = original_key.split('/')[-1].rsplit('.', 1)[0]
    
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

    This function is triggered when a PDF file is uploaded to an S3 bucket. It downloads the 
    file from S3, splits the PDF into chunks (based on a page size), uploads each chunk back 
    to S3, and starts an AWS Step Functions execution to process the chunks. The function 
    also logs the processing status of each chunk.

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

        # Get the PDF file from S3
        response = s3.get_object(Bucket=bucket_name, Key=pdf_file_key)
        print(f'Filename - {pdf_file_key} | The response is: {response}')
        pdf_file_content = response['Body'].read()
  
        # Split the PDF into pages and upload them to S3
        # Uses both page count (90 max) AND file size (95MB max) limits
        # Adobe API limit is 104MB, we use 95MB for safety margin
        chunks = split_pdf_into_pages(pdf_file_content, pdf_file_key, s3, bucket_name, 90, max_chunk_size_mb=95)
        
        log_chunk_created(file_basename)

        # Trigger Step Function with the list of chunks
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps({
                "chunks": chunks, 
                "s3_bucket": bucket_name,
                "original_pdf_key": pdf_file_key
            })
        )
        print(f"Filename - {pdf_file_key} | Step Function started: {response['executionArn']}")

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
