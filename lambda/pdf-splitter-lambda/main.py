"""
This AWS Lambda function is triggered by an S3 event when a PDF file is uploaded to a specified S3 bucket. 
The function performs the following operations:

1. Downloads the PDF file from S3.
2. Pre-scans the PDF using PyMuPDF to detect "risky" characteristics that may cause Adobe API failures.
3. HIGH-RISK PDFs (complexity score >= 50) are moved to pre-failed/ folder and NOT processed.
4. MEDIUM-RISK PDFs (score 25-49) are processed with smaller chunks.
5. LOW-RISK PDFs (score < 25) are processed normally.
6. Splits the PDF into chunks of specified page size (adjusted based on risk assessment).
7. Uploads each PDF chunk to a temporary location in the same S3 bucket.
8. Starts an AWS Step Functions execution with metadata about the uploaded chunks.

Folder structure is preserved: pdf/collection/file.pdf -> pre-failed/collection/file.pdf
"""
import json
import boto3
import urllib.parse
import io
import os
from datetime import datetime

# Initialize AWS clients
cloudwatch = boto3.client('cloudwatch')
s3_client = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

state_machine_arn = os.environ['STATE_MACHINE_ARN']

# Risk thresholds
HIGH_RISK_THRESHOLD = 50
MEDIUM_RISK_THRESHOLD = 25


def prescan_pdf_advanced(pdf_content: bytes, filename: str) -> dict:
    """
    Advanced pre-scan of a PDF using PyMuPDF to detect characteristics that may cause Adobe API failures.
    
    Risk factors detected with weighted scoring:
    - File size > 100MB: +50 points (exceeds Adobe limit)
    - File size > 50MB: +20 points
    - Page count > 200: +30 points
    - Page count > 100: +15 points
    - MB/page > 5: +30 points (very high density)
    - MB/page > 2: +15 points (high density)
    - Images over 4MP: +20 points
    - Images over 1MP (>5): +10 points
    - Pages larger than tabloid: +15 points
    - Multiple page sizes (>3): +10 points
    - Image-heavy (>3 images/page): +15 points
    - CMYK images: +5 points
    - Heavy vector graphics (>100 drawings/page): +20 points
    - JavaScript: +10 points
    - Embedded files: +5 points
    - Encrypted: +25 points
    - Possibly scanned (<100 chars/page with images): +15 points
    """
    import fitz  # PyMuPDF
    
    file_size = len(pdf_content)
    file_size_mb = file_size / (1024 * 1024)
    
    result = {
        'filename': filename,
        'file_size_bytes': file_size,
        'file_size_mb': round(file_size_mb, 2),
        'error': None,
        'page_count': 0,
        'pdf_version': '',
        'is_encrypted': False,
        'has_javascript': False,
        'has_embedded_files': False,
        'total_images': 0,
        'total_text_chars': 0,
        'total_fonts': 0,
        'total_drawings': 0,
        'image_colorspaces': {},
        'max_image_pixels': 0,
        'images_over_1mp': 0,
        'images_over_4mp': 0,
        'unique_page_sizes': 0,
        'pages_over_tabloid': 0,
        'mb_per_page': 0,
        'images_per_page': 0,
        'chars_per_page': 0,
        'drawings_per_page': 0,
        'risk_factors': [],
        'complexity_score': 0,
        'risk_level': 'LOW',
        'recommended_pages_per_chunk': 90,
    }
    
    try:
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        
        result['page_count'] = len(doc)
        result['pdf_version'] = f"{doc.metadata.get('format', 'Unknown')}"
        result['is_encrypted'] = doc.is_encrypted
        
        try:
            js = doc.get_page_javascripts()
            result['has_javascript'] = bool(js)
        except:
            pass
        
        try:
            result['has_embedded_files'] = doc.embfile_count() > 0
        except:
            pass
        
        all_fonts = set()
        page_sizes_set = set()
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            
            w, h = page.rect.width, page.rect.height
            norm_size = (round(min(w, h), 0), round(max(w, h), 0))
            page_sizes_set.add(norm_size)
            
            if w > 1224 or h > 1224:
                result['pages_over_tabloid'] += 1
            
            try:
                image_list = page.get_images(full=True)
                result['total_images'] += len(image_list)
                
                for img in image_list:
                    xref = img[0]
                    try:
                        base_image = doc.extract_image(xref)
                        if base_image:
                            img_width = base_image.get('width', 0)
                            img_height = base_image.get('height', 0)
                            img_colorspace = base_image.get('colorspace', 0)
                            
                            cs_names = {1: 'Gray', 3: 'RGB', 4: 'CMYK'}
                            cs_name = cs_names.get(img_colorspace, f'CS{img_colorspace}')
                            result['image_colorspaces'][cs_name] = result['image_colorspaces'].get(cs_name, 0) + 1
                            
                            pixels = img_width * img_height
                            result['max_image_pixels'] = max(result['max_image_pixels'], pixels)
                            
                            if pixels > 1000000:
                                result['images_over_1mp'] += 1
                            if pixels > 4000000:
                                result['images_over_4mp'] += 1
                    except:
                        pass
            except:
                pass
            
            try:
                text = page.get_text()
                result['total_text_chars'] += len(text)
            except:
                pass
            
            try:
                fonts = page.get_fonts()
                for font in fonts:
                    all_fonts.add(font[3] if len(font) > 3 else str(font))
            except:
                pass
            
            try:
                drawings = page.get_drawings()
                result['total_drawings'] += len(drawings)
            except:
                pass
        
        doc.close()
        
        result['total_fonts'] = len(all_fonts)
        result['unique_page_sizes'] = len(page_sizes_set)
        
        if result['page_count'] > 0:
            result['mb_per_page'] = round(file_size_mb / result['page_count'], 3)
            result['images_per_page'] = round(result['total_images'] / result['page_count'], 2)
            result['chars_per_page'] = round(result['total_text_chars'] / result['page_count'], 0)
            result['drawings_per_page'] = round(result['total_drawings'] / result['page_count'], 1)
        
        # Risk assessment with weighted scoring
        risk_factors = []
        complexity_score = 0
        
        if file_size_mb > 100:
            risk_factors.append(f"File exceeds 100MB limit ({file_size_mb:.1f}MB)")
            complexity_score += 50
        elif file_size_mb > 50:
            risk_factors.append(f"Large file ({file_size_mb:.1f}MB)")
            complexity_score += 20
        
        if result['page_count'] > 200:
            risk_factors.append(f"Over 200 pages ({result['page_count']})")
            complexity_score += 30
        elif result['page_count'] > 100:
            risk_factors.append(f"Over 100 pages ({result['page_count']})")
            complexity_score += 15
        
        if result['mb_per_page'] > 5:
            risk_factors.append(f"Very high density ({result['mb_per_page']:.1f} MB/page)")
            complexity_score += 30
        elif result['mb_per_page'] > 2:
            risk_factors.append(f"High density ({result['mb_per_page']:.1f} MB/page)")
            complexity_score += 15
        
        if result['images_over_4mp'] > 0:
            risk_factors.append(f"{result['images_over_4mp']} images over 4MP")
            complexity_score += 20
        if result['images_over_1mp'] > 5:
            risk_factors.append(f"{result['images_over_1mp']} images over 1MP")
            complexity_score += 10
        
        if result['pages_over_tabloid'] > 0:
            risk_factors.append(f"{result['pages_over_tabloid']} pages larger than tabloid")
            complexity_score += 15
        
        if result['unique_page_sizes'] > 3:
            risk_factors.append(f"{result['unique_page_sizes']} different page sizes")
            complexity_score += 10
        
        if result['images_per_page'] > 3:
            risk_factors.append(f"Image-heavy ({result['images_per_page']:.1f} images/page)")
            complexity_score += 15
        
        cmyk_count = result['image_colorspaces'].get('CMYK', 0)
        if cmyk_count > 0:
            risk_factors.append(f"{cmyk_count} CMYK images")
            complexity_score += 5
        
        if result['drawings_per_page'] > 100:
            risk_factors.append(f"Heavy vector graphics ({result['drawings_per_page']:.0f} drawings/page)")
            complexity_score += 20
        elif result['drawings_per_page'] > 50:
            risk_factors.append(f"Many vector graphics ({result['drawings_per_page']:.0f} drawings/page)")
            complexity_score += 10
        
        if result['has_javascript']:
            risk_factors.append("Contains JavaScript")
            complexity_score += 10
        
        if result['has_embedded_files']:
            risk_factors.append("Contains embedded files")
            complexity_score += 5
        
        if result['is_encrypted']:
            risk_factors.append("Encrypted PDF")
            complexity_score += 25
        
        if result['page_count'] > 0 and result['chars_per_page'] < 100 and result['images_per_page'] > 0:
            risk_factors.append(f"Possibly scanned (only {result['chars_per_page']:.0f} chars/page)")
            complexity_score += 15
        
        result['risk_factors'] = risk_factors
        result['complexity_score'] = complexity_score
        
        if complexity_score >= HIGH_RISK_THRESHOLD:
            result['risk_level'] = 'HIGH'
        elif complexity_score >= MEDIUM_RISK_THRESHOLD:
            result['risk_level'] = 'MEDIUM'
        else:
            result['risk_level'] = 'LOW'
        
        if result['risk_level'] == 'HIGH':
            result['recommended_pages_per_chunk'] = 1
        elif result['risk_level'] == 'MEDIUM':
            if result['page_count'] <= 10:
                result['recommended_pages_per_chunk'] = 1
            elif result['page_count'] <= 20:
                result['recommended_pages_per_chunk'] = 2
            else:
                result['recommended_pages_per_chunk'] = 5
        else:
            result['recommended_pages_per_chunk'] = 90
        
        print(f'Filename - {filename} | ADVANCED PRE-SCAN REPORT:')
        print(f'Filename - {filename} |   - Pages: {result["page_count"]}')
        print(f'Filename - {filename} |   - File size: {file_size_mb:.2f} MB ({result["mb_per_page"]:.2f} MB/page)')
        print(f'Filename - {filename} |   - Images: {result["total_images"]} ({result["images_per_page"]:.1f}/page)')
        print(f'Filename - {filename} |   - Max image: {result["max_image_pixels"]/1000000:.1f}MP')
        print(f'Filename - {filename} |   - Images >1MP: {result["images_over_1mp"]}, >4MP: {result["images_over_4mp"]}')
        print(f'Filename - {filename} |   - Text chars: {result["total_text_chars"]} ({result["chars_per_page"]:.0f}/page)')
        print(f'Filename - {filename} |   - Fonts: {result["total_fonts"]}')
        print(f'Filename - {filename} |   - Drawings: {result["total_drawings"]} ({result["drawings_per_page"]:.1f}/page)')
        print(f'Filename - {filename} |   - Page sizes: {result["unique_page_sizes"]} unique')
        print(f'Filename - {filename} |   - Pages over tabloid: {result["pages_over_tabloid"]}')
        print(f'Filename - {filename} |   - Colorspaces: {result["image_colorspaces"]}')
        print(f'Filename - {filename} |   - Complexity Score: {complexity_score}')
        print(f'Filename - {filename} |   - Risk Level: {result["risk_level"]}')
        if risk_factors:
            print(f'Filename - {filename} |   - Risk factors: {", ".join(risk_factors)}')
        print(f'Filename - {filename} |   - Recommended pages/chunk: {result["recommended_pages_per_chunk"]}')
        
    except Exception as e:
        print(f'Filename - {filename} | PRE-SCAN: Error during analysis: {e}')
        import traceback
        traceback.print_exc()
        result['error'] = str(e)
        result['risk_factors'] = [f"Error analyzing PDF: {e}"]
        result['complexity_score'] = 100
        result['risk_level'] = 'HIGH'
    
    return result


def move_to_pre_failed(s3_client, bucket_name, source_key, prescan_result):
    """
    Move a high-risk PDF to the pre-failed/ folder, preserving folder structure.
    
    Example: pdf/collection-a/document.pdf -> pre-failed/collection-a/document.pdf
    """
    if source_key.startswith('pdf/'):
        relative_path = source_key[4:]
    else:
        relative_path = source_key
    
    dest_key = f"pre-failed/{relative_path}"
    
    metadata = {
        'complexity-score': str(prescan_result['complexity_score']),
        'risk-level': prescan_result['risk_level'],
        'page-count': str(prescan_result['page_count']),
        'file-size-mb': str(prescan_result['file_size_mb']),
        'pre-failed-date': datetime.utcnow().isoformat(),
        'risk-factors': '; '.join(prescan_result['risk_factors'][:5])[:500],
    }
    
    copy_source = {'Bucket': bucket_name, 'Key': source_key}
    s3_client.copy_object(
        Bucket=bucket_name,
        Key=dest_key,
        CopySource=copy_source,
        Metadata=metadata,
        MetadataDirective='REPLACE'
    )
    
    s3_client.delete_object(Bucket=bucket_name, Key=source_key)
    
    print(f'Filename - {source_key} | MOVED TO PRE-FAILED: {dest_key}')
    print(f'Filename - {source_key} |   - Complexity Score: {prescan_result["complexity_score"]}')
    print(f'Filename - {source_key} |   - Risk Factors: {", ".join(prescan_result["risk_factors"])}')
    
    return dest_key


def log_chunk_created(filename):
    """Logs the creation of a PDF chunk."""
    print(f"File: {filename}, Status: Processing")
    print(f'Filename - {filename} | Uploaded {filename} to S3')
    return {'statusCode': 200, 'body': 'Metric status updated.'}


def split_pdf_into_pages(source_content, original_key, s3_client, bucket_name, pages_per_chunk, max_chunk_size_mb=95, retry_count=0):
    """
    Splits a PDF file into chunks based on page count AND file size limits using PyMuPDF.
    
    Adobe API has a 104MB limit, so we use 95MB as a safe threshold.
    """
    import fitz
    
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
    
    doc = fitz.open(stream=source_content, filetype="pdf")
    num_pages = len(doc)
    file_basename = original_key.split('/')[-1].rsplit('.', 1)[0]
    
    RETRY_PAGE_THRESHOLD = 10
    is_retry = retry_count > 0
    
    if is_retry and num_pages <= RETRY_PAGE_THRESHOLD:
        pages_per_chunk = 1
        print(f'Filename - {file_basename} | RETRY #{retry_count}: Using per-page splitting for {num_pages}-page PDF')
    elif is_retry:
        pages_per_chunk = min(pages_per_chunk, 5)
        print(f'Filename - {file_basename} | RETRY #{retry_count}: Using {pages_per_chunk} pages per chunk for {num_pages}-page PDF')
    
    original_pdf_key = original_key
    key_without_prefix = original_key.replace('pdf/', '', 1)
    folder_path = key_without_prefix.rsplit('/', 1)[0] if '/' in key_without_prefix else ''
    
    chunks = []
    chunk_index = 0
    current_page = 0
    
    while current_page < num_pages:
        chunk_index += 1
        end_page = min(current_page + pages_per_chunk, num_pages)
        
        chunk_created = False
        while not chunk_created and current_page < end_page:
            chunk_doc = fitz.open()
            chunk_doc.insert_pdf(doc, from_page=current_page, to_page=end_page - 1)
            
            output = io.BytesIO()
            chunk_doc.save(output)
            chunk_size = output.tell()
            output.seek(0)
            chunk_doc.close()
            
            pages_in_chunk = end_page - current_page
            
            if chunk_size <= max_chunk_size_bytes:
                page_filename = f"{file_basename}_chunk_{chunk_index}.pdf"
                folder_prefix = f"{folder_path}/" if folder_path else ""
                s3_key = f"temp/{folder_prefix}{file_basename}/{page_filename}"
                
                s3_client.upload_fileobj(Fileobj=output, Bucket=bucket_name, Key=s3_key)
                
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
                if pages_in_chunk <= 1:
                    page_filename = f"{file_basename}_chunk_{chunk_index}.pdf"
                    folder_prefix = f"{folder_path}/" if folder_path else ""
                    s3_key = f"temp/{folder_prefix}{file_basename}/{page_filename}"
                    
                    s3_client.upload_fileobj(Fileobj=output, Bucket=bucket_name, Key=s3_key)
                    
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
                    reduction = max(1, pages_in_chunk // 2)
                    end_page = current_page + (pages_in_chunk - reduction)
                    chunk_size_mb = chunk_size / (1024 * 1024)
                    print(f'Filename - {file_basename} | Chunk too large ({chunk_size_mb:.1f}MB), reducing from {pages_in_chunk} to {end_page - current_page} pages')
    
    doc.close()
    return chunks


def lambda_handler(event, context):
    """
    AWS Lambda function to handle S3 events and split uploaded PDF files into chunks.

    HIGH-RISK PDFs (score >= 50) are moved to pre-failed/ and NOT processed.
    MEDIUM-RISK PDFs (score 25-49) are processed with smaller chunks.
    LOW-RISK PDFs (score < 25) are processed normally.
    
    Folder structure is preserved in pre-failed/ folder.
    """
    try:
        print("Received event: " + json.dumps(event, indent=2))

        if 'Records' in event and len(event['Records']) > 0:
            s3_record = event['Records'][0]
            bucket_name = s3_record['s3']['bucket']['name']
            pdf_file_key = urllib.parse.unquote_plus(s3_record['s3']['object']['key'])
        else:
            raise ValueError("Event does not contain 'Records'. Check the S3 event structure.")
        
        file_basename = pdf_file_key.split('/')[-1].rsplit('.', 1)[0]

        s3 = boto3.client('s3')
        stepfunctions = boto3.client('stepfunctions')

        retry_count = 0
        try:
            head_response = s3.head_object(Bucket=bucket_name, Key=pdf_file_key)
            metadata = head_response.get('Metadata', {})
            retry_count = int(metadata.get('retry-count', '0'))
            if retry_count > 0:
                print(f'Filename - {pdf_file_key} | RETRY #{retry_count} detected via S3 metadata')
        except Exception as e:
            print(f'Filename - {pdf_file_key} | Could not read metadata: {e}')

        response = s3.get_object(Bucket=bucket_name, Key=pdf_file_key)
        print(f'Filename - {pdf_file_key} | Downloaded from S3')
        pdf_file_content = response['Body'].read()
        
        # Advanced pre-scan using PyMuPDF
        prescan_result = prescan_pdf_advanced(pdf_file_content, pdf_file_key)
        
        # HIGH-RISK: Move to pre-failed/ and do NOT process
        if prescan_result['risk_level'] == 'HIGH':
            print(f'Filename - {pdf_file_key} | HIGH-RISK PDF DETECTED (score={prescan_result["complexity_score"]})')
            print(f'Filename - {pdf_file_key} | Moving to pre-failed/ folder - will NOT be processed')
            
            pre_failed_key = move_to_pre_failed(s3, bucket_name, pdf_file_key, prescan_result)
            
            try:
                cloudwatch.put_metric_data(
                    Namespace='PDFRemediation',
                    MetricData=[{
                        'MetricName': 'PreFailedFiles',
                        'Value': 1,
                        'Unit': 'Count',
                        'Dimensions': [{'Name': 'RiskLevel', 'Value': 'HIGH'}]
                    }]
                )
            except Exception as e:
                print(f'Filename - {pdf_file_key} | Could not emit CloudWatch metric: {e}')
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'pre-failed',
                    'message': f'High-risk PDF moved to {pre_failed_key}',
                    'complexity_score': prescan_result['complexity_score'],
                    'risk_factors': prescan_result['risk_factors']
                })
            }
        
        # MEDIUM or LOW risk - proceed with processing
        prescan_pages_per_chunk = prescan_result['recommended_pages_per_chunk']
        
        default_pages_per_chunk = 90
        if retry_count > 0:
            num_pages = prescan_result['page_count']
            retry_pages_per_chunk = 1 if num_pages <= 10 else 5
        else:
            retry_pages_per_chunk = default_pages_per_chunk
        
        final_pages_per_chunk = min(prescan_pages_per_chunk, retry_pages_per_chunk)
        
        print(f'Filename - {pdf_file_key} | SPLITTING DECISION:')
        print(f'Filename - {pdf_file_key} |   - Risk Level: {prescan_result["risk_level"]}')
        print(f'Filename - {pdf_file_key} |   - Pre-scan recommendation: {prescan_pages_per_chunk} pages/chunk')
        print(f'Filename - {pdf_file_key} |   - Retry recommendation: {retry_pages_per_chunk} pages/chunk (retry_count={retry_count})')
        print(f'Filename - {pdf_file_key} |   - FINAL DECISION: {final_pages_per_chunk} pages/chunk')
        print(f'Filename - {pdf_file_key} |   - Total pages: {prescan_result["page_count"]}')
        print(f'Filename - {pdf_file_key} |   - Expected chunks: ~{max(1, prescan_result["page_count"] // final_pages_per_chunk)}')
  
        chunks = split_pdf_into_pages(pdf_file_content, pdf_file_key, s3, bucket_name, final_pages_per_chunk, max_chunk_size_mb=95, retry_count=0)
        
        print(f'Filename - {pdf_file_key} | SPLITTING COMPLETE: Created {len(chunks)} chunk(s) from {prescan_result["page_count"]} pages')
        
        log_chunk_created(file_basename)

        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps({
                "chunks": chunks, 
                "s3_bucket": bucket_name,
                "original_pdf_key": pdf_file_key,
                "retry_count": retry_count,
                "prescan": {
                    "is_risky": prescan_result['risk_level'] != 'LOW',
                    "risk_level": prescan_result['risk_level'],
                    "complexity_score": prescan_result['complexity_score'],
                    "risk_factors": prescan_result['risk_factors'],
                    "pages_per_chunk": final_pages_per_chunk
                }
            })
        )
        print(f"Filename - {pdf_file_key} | Step Function started: {response['executionArn']} | retry_count: {retry_count} | risk_level: {prescan_result['risk_level']} | pages_per_chunk: {final_pages_per_chunk}")

    except KeyError as e:
        print(f"File: {file_basename}, Status: Failed in split lambda function")
        print(f"Filename - {pdf_file_key} | KeyError: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: Missing key in event: {str(e)}")}
    except ValueError as e:
        print(f"File: {file_basename}, Status: Failed in split lambda function")
        print(f"Filename - {pdf_file_key} | ValueError: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}
    except Exception as e:
        print(f"File: {file_basename}, Status: Failed in split lambda function")
        print(f"Filename - {pdf_file_key} | Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'statusCode': 500, 'body': json.dumps(f"Error processing event: {str(e)}")}

    return {'statusCode': 200, 'body': json.dumps('Event processed successfully!')}

