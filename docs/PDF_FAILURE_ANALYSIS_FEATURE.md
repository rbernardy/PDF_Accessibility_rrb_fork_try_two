# PDF Failure Analysis Feature

## Overview

This feature adds automated analysis of PDFs that fail during Adobe API processing (autotag/extract). When a PDF fails with an Adobe API error (excluding rate limit 429 errors), a Lambda function analyzes the PDF structure to identify likely causes of failure.

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│  Adobe Autotag      │     │  PDF Failure         │     │  CloudWatch Logs    │
│  Processor (ECS)    │────▶│  Analysis Lambda     │────▶│  + S3 Report        │
│  (on API error)     │     │  (Docker w/ PyMuPDF) │     │                     │
└─────────────────────┘     └──────────────────────┘     └─────────────────────┘
```

## Trigger Conditions

The analysis Lambda is triggered when:
- Adobe Autotag API fails (non-429 error)
- Adobe Extract API fails (non-429 error)

It is NOT triggered for:
- Rate limit errors (429 Too Many Requests)
- Successful processing
- Non-Adobe errors (S3, network, etc.)

## Analysis Performed

### Using PyMuPDF (Primary - Pure Python)

| Check | Description | Failure Indicator |
|-------|-------------|-------------------|
| Page Count | Total pages in document | > 500 pages |
| File Size | Raw file size | > 100 MB |
| Page Dimensions | Width/height of each page | Unusual sizes, mixed sizes |
| Image Count | Total embedded images | > 200 images |
| Image Sizes | Dimensions of each image | > 4000px dimension |
| Font Count | Number of fonts used | > 50 fonts |
| Font Embedding | Whether fonts are embedded | Missing/subset fonts |
| Encryption | Password protection | Any encryption |
| PDF Version | PDF specification version | Very old (< 1.4) or very new |
| Annotations | Form fields, comments, etc. | > 100 annotations |
| Layers | Optional content groups | Complex layering |
| Metadata | Document properties | Corrupted metadata |

### Using Poppler Tools (Alternative - CLI)

If Poppler is preferred, these CLI tools provide similar analysis:

| Tool | Purpose |
|------|---------|
| `pdfinfo` | Page count, PDF version, encryption, page sizes, metadata |
| `pdffonts` | Font listing, embedding status, font types |
| `pdfimages -list` | Image inventory with dimensions and color space |
| `pdftotext -layout` | Text extraction validation |

## Output

### CloudWatch Log Entry (Structured JSON)

```json
{
  "event_type": "PDF_FAILURE_ANALYSIS",
  "filename": "example.pdf",
  "s3_bucket": "my-bucket",
  "s3_key": "input/example.pdf",
  "api_type": "autotag",
  "original_error": "ServiceApiException: Invalid PDF structure",
  "analysis": {
    "file_size_mb": 45.2,
    "page_count": 127,
    "image_count": 89,
    "font_count": 12,
    "has_encryption": false,
    "pdf_version": "1.7",
    "issues": [
      {
        "severity": "HIGH",
        "category": "IMAGE_SIZE",
        "description": "3 images exceed 4000px dimension",
        "details": ["page 12: 5000x3000", "page 45: 4500x4500", "page 89: 6000x4000"]
      },
      {
        "severity": "MEDIUM", 
        "category": "PAGE_COUNT",
        "description": "Document has 127 pages, may cause timeout"
      }
    ],
    "likely_cause": "Large images on pages 12, 45, 89 likely caused processing failure"
  },
  "timestamp": "2026-02-23T10:30:00Z"
}
```

### S3 Reports

Reports are saved to S3 in two formats:

1. **Text Report** (human-readable):
   ```
   s3://{bucket}/reports/failure_analysis/{filename}_analysis_{timestamp}.txt
   ```

2. **Word Document** (formatted report):
   ```
   s3://{bucket}/reports/failure_analysis/{filename}_analysis_{timestamp}.docx
   ```

### CloudWatch Dashboard Widget

A "PDF Failure Analysis" widget is added to the dashboard showing:
- Timestamp
- Filename
- API Type (autotag/extract)
- File Size (MB)
- Page Count
- Image Count
- Likely Cause

## Implementation Options

### Option A: PyMuPDF (Recommended)

**Pros:**
- Pure Python, no external dependencies
- Faster cold starts
- Already used in the project
- Rich API for PDF inspection

**Cons:**
- Some edge cases may not be detected

### Option B: Poppler Tools

**Pros:**
- Industry-standard PDF tools
- Very thorough analysis
- Handles malformed PDFs well

**Cons:**
- Requires custom Docker image
- Larger image size (~100MB more)
- Slower cold starts

### Option C: Hybrid (PyMuPDF + Poppler fallback)

Use PyMuPDF for primary analysis, fall back to Poppler for PDFs that PyMuPDF can't open.

## Lambda Configuration

| Setting | Value |
|---------|-------|
| Runtime | Python 3.12 (Docker) |
| Memory | 1024 MB |
| Timeout | 60 seconds |
| Architecture | ARM64 |
| Ephemeral Storage | 1024 MB (for large PDFs) |

## Integration Points

### Trigger from Adobe Autotag Processor

The autotag processor will invoke this Lambda when catching Adobe API exceptions:

```python
except (ServiceApiException, ServiceUsageException) as e:
    error_str = str(e)
    # Skip rate limit errors - those are handled by retry logic
    if '429' not in error_str and 'Too Many Requests' not in error_str:
        invoke_failure_analysis(bucket, key, filename, error_str)
    raise
```

### SNS/EventBridge Alternative

Alternatively, failures can be published to SNS/EventBridge and the analysis Lambda subscribes to those events.

## Future Enhancements

1. **Auto-remediation suggestions** - Recommend specific fixes (resize images, split document, etc.)
2. **Pre-processing validation** - Run analysis BEFORE sending to Adobe API to reject problematic PDFs early
3. **Dashboard widget** - Show failure analysis results in CloudWatch dashboard
4. **Notification integration** - Include analysis in failure digest emails

## Files

| File | Description |
|------|-------------|
| `lambda/pdf-failure-analysis/main.py` | Lambda handler |
| `lambda/pdf-failure-analysis/Dockerfile` | Docker image with PyMuPDF (default) |
| `lambda/pdf-failure-analysis/Dockerfile.with-poppler` | Alternative Docker image with Poppler tools |
| `lambda/pdf-failure-analysis/requirements.txt` | Python dependencies |
| `lambda/pdf-failure-analysis/analyzer.py` | PyMuPDF-based PDF analysis logic |
| `lambda/pdf-failure-analysis/poppler_analyzer.py` | Poppler CLI-based analysis (optional) |

## CDK Integration (Not Yet Added)

To add this Lambda to the stack, the following needs to be added to `app.py`:

```python
# PDF Failure Analysis Lambda
pdf_failure_analysis_lambda = lambda_.DockerImageFunction(
    self, "PdfFailureAnalysisLambda",
    function_name="pdf-failure-analysis",
    code=lambda_.DockerImageCode.from_image_asset("lambda/pdf-failure-analysis"),
    memory_size=1024,
    timeout=Duration.seconds(60),
    ephemeral_storage_size=cdk.Size.mebibytes(1024),
    architecture=lambda_.Architecture.ARM_64,
    environment={
        "REPORT_BUCKET": source_bucket_name,
        "SAVE_REPORTS_TO_S3": "true"
    }
)

# Grant S3 read access for downloading PDFs
source_bucket.grant_read(pdf_failure_analysis_lambda)
# Grant S3 write access for saving reports (optional)
source_bucket.grant_write(pdf_failure_analysis_lambda, "failure-reports/*")
```

Then invoke from the autotag processor on failure:

```python
# In adobe_autotag_processor.py
lambda_client = boto3.client('lambda')

def invoke_failure_analysis(bucket: str, key: str, filename: str, error: str):
    """Invoke the PDF failure analysis Lambda."""
    try:
        lambda_client.invoke(
            FunctionName='pdf-failure-analysis',
            InvocationType='Event',  # Async
            Payload=json.dumps({
                'bucket': bucket,
                'key': key,
                'filename': filename,
                'original_error': str(error),
                'api_type': 'autotag'
            })
        )
    except Exception as e:
        logging.warning(f"Failed to invoke failure analysis: {e}")
```
