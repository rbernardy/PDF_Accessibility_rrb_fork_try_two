# Failure Report Web UI - Design

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  CloudFront     │────▶│  S3 Bucket      │     │  Static Assets  │
│  Distribution   │     │  (Website)      │     │  HTML/CSS/JS    │
│                 │     │                 │     │                 │
└────────┬────────┘     └─────────────────┘     └─────────────────┘
         │
         │ /api/*
         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  API Gateway    │────▶│  Lambda         │────▶│  DynamoDB       │
│  REST API       │     │  (Report Gen)   │     │  (Failure Data) │
│                 │     │                 │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │                 │
                        │  S3 Bucket      │
                        │  (Reports)      │
                        │                 │
                        └─────────────────┘
```

## Components

### 1. Static Website (S3 + CloudFront)

**S3 Bucket Configuration:**
- Bucket: Reuse existing `pdf-processing-bucket` or create dedicated bucket
- Path: `web/failure-report/`
- Files: `index.html`, `styles.css`, `app.js`

**CloudFront Distribution:**
- Origin 1: S3 bucket (default, for static files)
- Origin 2: API Gateway (for `/api/*` paths)
- Behavior: Route `/api/*` to API Gateway origin
- HTTPS only, redirect HTTP

### 2. API Gateway

**REST API Endpoints:**

```
GET  /api/filters
POST /api/generate
```

**GET /api/filters Response:**
```json
{
  "collection_folders": [
    "pdfs-test-failures-13-american_boy-S0-F19",
    "pdfs-test-failures-07-coe_college-S44-F59"
  ],
  "error_codes": [
    "INTERNAL_SERVER_ERROR",
    "TIMEOUT",
    "BAD_REQUEST"
  ],
  "api_types": ["autotag", "extract"]
}
```

**POST /api/generate Request:**
```json
{
  "collection_folder": "pdfs-test-failures-13-american_boy-S0-F19",
  "error_codes": ["INTERNAL_SERVER_ERROR"],
  "api_type": "both",
  "date_from": "2026-03-01T00:00:00Z",
  "date_to": "2026-03-11T23:59:59Z",
  "crashed_only": false
}
```

**POST /api/generate Response:**
```json
{
  "status": "success",
  "record_count": 42,
  "download_url": "https://bucket.s3.amazonaws.com/reports/...?X-Amz-Signature=...",
  "expires_in": 3600
}
```

### 3. Lambda Function

**Option A: Extend Existing Lambda**
- Add new handler function for filtered queries
- Reuse `create_excel_report()` function
- Add `get_filter_options()` function

**Option B: New Lambda (Recommended)**
- Dedicated Lambda: `failure-report-api`
- Cleaner separation of concerns
- Can have different timeout/memory settings

**Lambda Logic:**
```python
def handler(event, context):
    path = event['path']
    method = event['httpMethod']
    
    if path == '/api/filters' and method == 'GET':
        return get_filter_options()
    elif path == '/api/generate' and method == 'POST':
        body = json.loads(event['body'])
        return generate_filtered_report(body)
    else:
        return {'statusCode': 404}
```

### 4. Web Form UI

**HTML Structure:**
```html
<form id="report-form">
  <div class="form-group">
    <label>Collection Folder</label>
    <select id="collection-folder">
      <option value="">All Folders</option>
      <!-- Populated dynamically -->
    </select>
  </div>
  
  <div class="form-group">
    <label>Error Codes</label>
    <div id="error-codes">
      <!-- Checkboxes populated dynamically -->
    </div>
  </div>
  
  <div class="form-group">
    <label>API Type</label>
    <input type="radio" name="api-type" value="both" checked> Both
    <input type="radio" name="api-type" value="autotag"> Autotag
    <input type="radio" name="api-type" value="extract"> Extract
  </div>
  
  <div class="form-group">
    <label>Date Range</label>
    <input type="date" id="date-from">
    <input type="date" id="date-to">
  </div>
  
  <div class="form-group">
    <input type="checkbox" id="crashed-only">
    <label>Crashed Only</label>
  </div>
  
  <button type="submit">Generate Report</button>
  <div id="status"></div>
</form>
```

## CDK Infrastructure

**New Resources:**
```python
# API Gateway
api = apigateway.RestApi(self, "FailureReportApi")

# Lambda
api_lambda = lambda_.Function(
    self, "FailureReportApiLambda",
    runtime=lambda_.Runtime.PYTHON_3_12,
    handler="main.handler",
    timeout=Duration.seconds(60),
    memory_size=512
)

# API Gateway Integration
api.root.add_resource("api").add_resource("filters").add_method(
    "GET", apigateway.LambdaIntegration(api_lambda)
)
api.root.add_resource("api").add_resource("generate").add_method(
    "POST", apigateway.LambdaIntegration(api_lambda)
)

# CloudFront
distribution = cloudfront.Distribution(
    self, "FailureReportDistribution",
    default_behavior=cloudfront.BehaviorOptions(
        origin=origins.S3Origin(website_bucket)
    ),
    additional_behaviors={
        "/api/*": cloudfront.BehaviorOptions(
            origin=origins.RestApiOrigin(api)
        )
    }
)
```

## Security Considerations

1. **CORS**: API Gateway configured to allow requests from CloudFront domain only
2. **Throttling**: API Gateway rate limit of 10 req/sec to prevent abuse
3. **Presigned URLs**: Report download URLs expire after 1 hour
4. **No Auth**: Acceptable for internal tool; URL not publicly advertised

## File Structure

```
lambda/
  failure-report-api/
    main.py           # API handler
    requirements.txt
web/
  failure-report/
    index.html        # Main form page
    styles.css        # Styling
    app.js            # Form logic, API calls
```

## Cost Breakdown

| Service | Pricing | Estimated Monthly |
|---------|---------|-------------------|
| CloudFront | $0.085/GB, 1M requests free | $0-1 |
| API Gateway | $3.50/million requests | $0.50 |
| Lambda | $0.20/million invocations | $0.10 |
| S3 | $0.023/GB storage | $0.01 |
| **Total** | | **~$1-2/month** |
