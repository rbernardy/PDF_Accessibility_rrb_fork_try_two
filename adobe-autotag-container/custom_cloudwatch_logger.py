"""
Custom CloudWatch Logger for PDF Processing Metrics

Provides structured logging to dedicated CloudWatch log streams:
- adobe-api-calls: Records filename, page count, file size, datetime for each Adobe API call
- adobe-api-errors: Records errors returned from Adobe API
- processing-failures: Records any PDF that fails with details on where it failed
"""

import boto3
import json
import os
import time
from datetime import datetime
from botocore.exceptions import ClientError


class CustomCloudWatchLogger:
    """
    Logger that writes to custom CloudWatch log streams for PDF processing metrics.
    """
    
    LOG_GROUP_NAME = "/custom/pdf-remediation/metrics"
    
    # Stream names
    STREAM_API_CALLS = "adobe-api-calls"
    STREAM_API_ERRORS = "adobe-api-errors"
    STREAM_PROCESSING_FAILURES = "processing-failures"
    
    def __init__(self):
        self.logs_client = boto3.client('logs')
        self._stream_tokens = {}
        self._initialized_streams = set()
    
    def _ensure_stream(self, stream_name: str) -> None:
        """Create log stream if it doesn't exist."""
        if stream_name in self._initialized_streams:
            return
        
        try:
            self.logs_client.create_log_stream(
                logGroupName=self.LOG_GROUP_NAME,
                logStreamName=stream_name
            )
        except self.logs_client.exceptions.ResourceAlreadyExistsException:
            pass
        except ClientError as e:
            # Log group might not exist yet during first deployment
            print(f"Warning: Could not create log stream {stream_name}: {e}")
            return
        
        self._initialized_streams.add(stream_name)
    
    def _put_log_event(self, stream_name: str, message: str) -> None:
        """Write a log event to the specified stream."""
        self._ensure_stream(stream_name)
        
        try:
            kwargs = {
                'logGroupName': self.LOG_GROUP_NAME,
                'logStreamName': stream_name,
                'logEvents': [{
                    'timestamp': int(time.time() * 1000),
                    'message': message
                }]
            }
            
            # Include sequence token if we have one (required for subsequent puts)
            if stream_name in self._stream_tokens:
                kwargs['sequenceToken'] = self._stream_tokens[stream_name]
            
            response = self.logs_client.put_log_events(**kwargs)
            self._stream_tokens[stream_name] = response.get('nextSequenceToken')
            
        except self.logs_client.exceptions.InvalidSequenceTokenException as e:
            # Token was stale, get the correct one and retry
            expected_token = str(e).split("sequenceToken is: ")[-1].strip()
            if expected_token and expected_token != "null":
                self._stream_tokens[stream_name] = expected_token
                self._put_log_event(stream_name, message)
        except ClientError as e:
            print(f"Warning: Could not write to CloudWatch stream {stream_name}: {e}")
    
    def log_adobe_api_call(self, filename: str, file_path: str, api_type: str = "autotag") -> None:
        """
        Log Adobe API call details.
        
        Args:
            filename: Name of the PDF file
            file_path: Path to the PDF file (for size/page extraction)
            api_type: Type of API call ('autotag' or 'extract')
        """
        try:
            from pypdf import PdfReader
            reader = PdfReader(file_path)
            page_count = len(reader.pages)
        except Exception:
            page_count = "unknown"
        
        try:
            file_size = os.path.getsize(file_path)
        except Exception:
            file_size = "unknown"
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "filename": filename,
            "api_type": api_type,
            "page_count": page_count,
            "file_size_bytes": file_size
        }
        
        self._put_log_event(self.STREAM_API_CALLS, json.dumps(log_entry))
    
    def log_adobe_api_error(self, filename: str, api_type: str, error: Exception) -> None:
        """
        Log Adobe API errors.
        
        Args:
            filename: Name of the PDF file
            api_type: Type of API call that failed ('autotag' or 'extract')
            error: The exception that occurred
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "filename": filename,
            "api_type": api_type,
            "error_type": type(error).__name__,
            "error_message": str(error)
        }
        
        self._put_log_event(self.STREAM_API_ERRORS, json.dumps(log_entry))
    
    def log_processing_failure(self, filename: str, file_path: str, stage: str, error: Exception) -> None:
        """
        Log processing failures with context.
        
        Args:
            filename: Name of the PDF file
            file_path: Path to the PDF file
            stage: Processing stage where failure occurred (e.g., 'download', 'autotag', 'extract', 'toc', 'upload')
            error: The exception that occurred
        """
        try:
            from pypdf import PdfReader
            reader = PdfReader(file_path)
            page_count = len(reader.pages)
        except Exception:
            page_count = "unknown"
        
        try:
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else "unknown"
        except Exception:
            file_size = "unknown"
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "filename": filename,
            "file_size_bytes": file_size,
            "page_count": page_count,
            "failed_stage": stage,
            "error_type": type(error).__name__,
            "error_message": str(error)
        }
        
        self._put_log_event(self.STREAM_PROCESSING_FAILURES, json.dumps(log_entry))


# Global instance for easy import
_logger_instance = None

def get_custom_logger() -> CustomCloudWatchLogger:
    """Get or create the singleton custom logger instance."""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = CustomCloudWatchLogger()
    return _logger_instance
