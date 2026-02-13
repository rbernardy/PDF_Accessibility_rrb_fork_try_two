import os
import boto3
import json
import traceback
from datetime import datetime, timezone
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException
from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
from adobe.pdfservices.operation.io.stream_asset import StreamAsset
from adobe.pdfservices.operation.pdf_services import PDFServices,ClientConfig
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.pdf_accessibility_checker_job import PDFAccessibilityCheckerJob
from adobe.pdfservices.operation.pdfjobs.result.pdf_accessibility_checker_result import PDFAccessibilityCheckerResult
from botocore.exceptions import ClientError


def log_error_to_s3(bucket_name: str, file_key: str, folder_path: str, error_type: str, error_message: str):
    """
    Log error details to S3 for tracking failed pre-remediation checks.
    
    Args:
        bucket_name: S3 bucket name
        file_key: The PDF filename
        folder_path: The folder path
        error_type: Type of error (e.g., 'AdobeAPIError', 'DownloadError')
        error_message: The error message
    """
    try:
        s3 = boto3.client('s3')
        file_key_without_extension = os.path.splitext(file_key)[0]
        folder_prefix = f"{folder_path}/" if folder_path else ""
        
        error_log = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "filename": file_key,
            "error_type": error_type,
            "error_message": str(error_message),
            "stage": "pre-remediation-accessibility-check"
        }
        
        error_path = f"temp/{folder_prefix}{file_key_without_extension}/accessability-report/{file_key_without_extension}_pre_remediation_ERROR.json"
        
        s3.put_object(
            Bucket=bucket_name,
            Key=error_path,
            Body=json.dumps(error_log, indent=2),
            ContentType='application/json'
        )
        print(f"PRE_REMEDIATION_ERROR: {json.dumps(error_log)}")
        print(f"Filename : {file_key} | Error log saved to {error_path}")
    except Exception as e:
        print(f"Filename : {file_key} | Failed to save error log: {e}")


def create_json_output_file_path():
        os.makedirs("/tmp/PDFAccessibilityChecker", exist_ok=True)
        return f"/tmp/PDFAccessibilityChecker/result_before_remediation.json"

def download_file_from_s3(bucket_name, file_key, local_path, original_pdf_key):
    s3 = boto3.client('s3')
    print(f"Filename : {file_key} | File key in the function: {file_key}")

    s3.download_file(bucket_name, original_pdf_key, local_path)

    print(f"Filename : {file_key} | Downloaded {file_key} from {bucket_name} to {local_path}")

def save_to_s3(bucket_name, file_key, folder_path=""):
    s3 = boto3.client('s3')
    local_path = "/tmp/PDFAccessibilityChecker/result_before_remediation.json"
    file_key_without_extension = os.path.splitext(file_key)[0]
    folder_prefix = f"{folder_path}/" if folder_path else ""
    bucket_save_path = f"temp/{folder_prefix}{file_key_without_extension}/accessability-report/{file_key_without_extension}_accessibility_report_before_remidiation.json"
    with open(local_path, "rb") as data:
        s3.upload_fileobj(data, bucket_name, bucket_save_path)
    print(f"Filename {file_key} | Uploaded {file_key} to {bucket_name} at path {bucket_save_path} before remidiation")
    return bucket_save_path

        
def get_secret(basefilename):
    secret_name = "/myapp/client_credentials"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret)
        
        client_id = secret_dict['client_credentials']['PDF_SERVICES_CLIENT_ID']
        client_secret = secret_dict['client_credentials']['PDF_SERVICES_CLIENT_SECRET']
        return client_id, client_secret

    except ClientError as e:
        print(f'Filename : {basefilename} | Error: {e}')
        raise  # Re-raise the exception to indicate failure

    except KeyError as e:
        print(f"Filename : {basefilename} | KeyError: Missing key in the secret data: {e}")
        raise  # Re-raise KeyError to indicate malformed secret structure

    except Exception as e:
        print(f"Filename : {basefilename} | Unexpected error: {e}")
        raise  # Re-raise unexpected exceptions for debugging
     

def lambda_handler(event, context):
    print("Received event:", event)
    s3_bucket = event.get('s3_bucket', None)
    chunks = event.get('chunks', [])
    
    # Extract original_pdf_key and folder_path from first chunk
    original_pdf_key = None
    folder_path = ''
    file_basename = 'unknown'
    
    try:
        if chunks:
            first_chunk = chunks[0]
            s3_key = first_chunk.get('s3_key', None)
            original_pdf_key = first_chunk.get('original_pdf_key', None)
            folder_path = first_chunk.get('folder_path', '')
            if s3_key:
                file_basename = os.path.basename(s3_key)
                file_basename = file_basename.split("_chunk_")[0] + os.path.splitext(file_basename)[1]
        else:
            error_msg = "No chunks provided in event"
            print(f"PRE_REMEDIATION_ERROR: {error_msg}")
            return {"status": "error", "message": error_msg}
                
        print("File basename:", file_basename)
        print("Original PDF key:", original_pdf_key)
        print("Folder path:", folder_path)
        print("s3_bucket:", s3_bucket)
        
        if not s3_bucket or not original_pdf_key:
            error_msg = f"Missing required parameters: s3_bucket={s3_bucket}, original_pdf_key={original_pdf_key}"
            print(f"PRE_REMEDIATION_ERROR: {error_msg}")
            log_error_to_s3(s3_bucket or 'unknown', file_basename, folder_path, 'ValidationError', error_msg)
            return {"status": "error", "message": error_msg}
        
        local_path = f"/tmp/{file_basename}"
        
        # Download file with error handling
        try:
            download_file_from_s3(s3_bucket, file_basename, local_path, original_pdf_key)
        except Exception as e:
            error_msg = f"Failed to download PDF from S3: {e}"
            print(f"Filename : {file_basename} | {error_msg}")
            print(f"PRE_REMEDIATION_ERROR: {json.dumps({'filename': file_basename, 'error': error_msg})}")
            log_error_to_s3(s3_bucket, file_basename, folder_path, 'S3DownloadError', error_msg)
            return {"status": "error", "filename": file_basename, "message": error_msg}

        # Get credentials with error handling
        try:
            client_id, client_secret = get_secret(file_basename)
        except Exception as e:
            error_msg = f"Failed to get Adobe API credentials: {e}"
            print(f"Filename : {file_basename} | {error_msg}")
            print(f"PRE_REMEDIATION_ERROR: {json.dumps({'filename': file_basename, 'error': error_msg})}")
            log_error_to_s3(s3_bucket, file_basename, folder_path, 'CredentialsError', error_msg)
            return {"status": "error", "filename": file_basename, "message": error_msg}

        # Run Adobe Accessibility Checker
        try:
            pdf_file = open(local_path, 'rb')
            input_stream = pdf_file.read()
            pdf_file.close()
            
            client_config = ClientConfig(
                connect_timeout=8000,
                read_timeout=40000
            )
            
            # Initial setup, create credentials instance
            credentials = ServicePrincipalCredentials(
                client_id=client_id,
                client_secret=client_secret)

            # Creates a PDF Services instance
            pdf_services = PDFServices(credentials=credentials, client_config=client_config)

            # Creates an asset(s) from source file(s) and upload
            print(f"Filename : {file_basename} | Uploading PDF to Adobe API...")
            input_asset = pdf_services.upload(input_stream=input_stream, mime_type=PDFServicesMediaType.PDF)

            # Creates a new job instance
            print(f"Filename : {file_basename} | Submitting accessibility check job...")
            pdf_accessibility_checker_job = PDFAccessibilityCheckerJob(input_asset=input_asset)

            # Submit the job and gets the job result
            location = pdf_services.submit(pdf_accessibility_checker_job)
            print(f"Filename : {file_basename} | Waiting for job result...")
            pdf_services_response = pdf_services.get_job_result(location, PDFAccessibilityCheckerResult)

            # Get content from the resulting asset(s)
            report_asset: CloudAsset = pdf_services_response.get_result().get_report()
            stream_report: StreamAsset = pdf_services.get_content(report_asset)
            output_file_path_json = create_json_output_file_path()
            with open(output_file_path_json, "wb") as file:
                file.write(stream_report.get_input_stream())
            
            bucket_save_path = save_to_s3(s3_bucket, file_basename, folder_path)
            print(f"Filename : {file_basename} | Saved accessibility report to {bucket_save_path}")
            print(f"File: {file_basename}, Status: Pre-remediation check completed successfully")
            
            return {
                "status": "success",
                "filename": file_basename,
                "report_path": bucket_save_path
            }

        except ServiceApiException as e:
            error_msg = f"Adobe API error: {e}"
            print(f"Filename : {file_basename} | {error_msg}")
            print(f"PRE_REMEDIATION_ERROR: {json.dumps({'filename': file_basename, 'error_type': 'ServiceApiException', 'error': str(e)})}")
            log_error_to_s3(s3_bucket, file_basename, folder_path, 'AdobeServiceApiError', error_msg)
            return {"status": "error", "filename": file_basename, "error_type": "ServiceApiException", "message": error_msg}
            
        except ServiceUsageException as e:
            error_msg = f"Adobe API usage error (possibly rate limit): {e}"
            print(f"Filename : {file_basename} | {error_msg}")
            print(f"PRE_REMEDIATION_ERROR: {json.dumps({'filename': file_basename, 'error_type': 'ServiceUsageException', 'error': str(e)})}")
            log_error_to_s3(s3_bucket, file_basename, folder_path, 'AdobeRateLimitError', error_msg)
            return {"status": "error", "filename": file_basename, "error_type": "ServiceUsageException", "message": error_msg}
            
        except SdkException as e:
            error_msg = f"Adobe SDK error: {e}"
            print(f"Filename : {file_basename} | {error_msg}")
            print(f"PRE_REMEDIATION_ERROR: {json.dumps({'filename': file_basename, 'error_type': 'SdkException', 'error': str(e)})}")
            log_error_to_s3(s3_bucket, file_basename, folder_path, 'AdobeSdkError', error_msg)
            return {"status": "error", "filename": file_basename, "error_type": "SdkException", "message": error_msg}

    except Exception as e:
        error_msg = f"Unexpected error: {e}\n{traceback.format_exc()}"
        print(f"Filename : {file_basename} | {error_msg}")
        print(f"PRE_REMEDIATION_ERROR: {json.dumps({'filename': file_basename, 'error_type': 'UnexpectedError', 'error': str(e)})}")
        if s3_bucket:
            log_error_to_s3(s3_bucket, file_basename, folder_path, 'UnexpectedError', error_msg)
        return {"status": "error", "filename": file_basename, "message": error_msg}
    
