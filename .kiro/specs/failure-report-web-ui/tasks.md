# Failure Report Web UI - Implementation Tasks

## Phase 1: Lambda API

### Task 1.1: Create API Lambda Function
- [ ] Create `lambda/failure-report-api/` directory
- [ ] Create `main.py` with handler routing
- [ ] Create `requirements.txt` (boto3, openpyxl)
- [ ] Implement `get_filter_options()` function
  - Query DynamoDB for distinct collection folders
  - Query DynamoDB for distinct error codes
  - Return JSON response

### Task 1.2: Implement Filtered Report Generation
- [ ] Copy/adapt `create_excel_report()` from existing Lambda
- [ ] Implement `generate_filtered_report()` function
  - Accept filter parameters from request body
  - Build DynamoDB FilterExpression based on filters
  - Generate Excel report
  - Upload to S3
  - Return presigned URL

### Task 1.3: Add DynamoDB Query Filters
- [ ] Filter by `collection_folder` (extracted from s3_key)
- [ ] Filter by `error_code` (parsed from original_error)
- [ ] Filter by `api_type`
- [ ] Filter by `analysis_timestamp` date range
- [ ] Filter by `crashed` status

## Phase 2: API Gateway

### Task 2.1: Create API Gateway in CDK
- [ ] Add REST API resource to `app.py`
- [ ] Create `/api/filters` GET endpoint
- [ ] Create `/api/generate` POST endpoint
- [ ] Configure Lambda integration
- [ ] Enable CORS for CloudFront domain

### Task 2.2: Configure API Gateway Settings
- [ ] Set throttling limits (10 req/sec)
- [ ] Configure request/response models
- [ ] Add error response mappings
- [ ] Set Lambda timeout to 60 seconds

## Phase 3: Static Website

### Task 3.1: Create HTML Form
- [ ] Create `web/failure-report/index.html`
- [ ] Add form structure with all filter fields
- [ ] Add status/loading indicators
- [ ] Add download link placeholder

### Task 3.2: Create Styling
- [ ] Create `web/failure-report/styles.css`
- [ ] Style form elements
- [ ] Add responsive layout
- [ ] Style loading spinner
- [ ] Style success/error messages

### Task 3.3: Create JavaScript Logic
- [ ] Create `web/failure-report/app.js`
- [ ] Implement `loadFilters()` - fetch and populate dropdowns
- [ ] Implement `generateReport()` - submit form, handle response
- [ ] Implement `downloadReport()` - trigger file download
- [ ] Add error handling and user feedback

## Phase 4: CloudFront & S3 Hosting

### Task 4.1: Configure S3 for Static Hosting
- [ ] Add S3 bucket policy for CloudFront access
- [ ] Upload static files to `web/failure-report/` path
- [ ] Configure index document

### Task 4.2: Create CloudFront Distribution in CDK
- [ ] Add CloudFront distribution resource
- [ ] Configure S3 origin for static files
- [ ] Configure API Gateway origin for `/api/*`
- [ ] Set up origin request policies
- [ ] Configure cache behaviors

### Task 4.3: Output CloudFront URL
- [ ] Add CDK output for distribution URL
- [ ] Document URL in README

## Phase 5: Testing & Documentation

### Task 5.1: Test API Endpoints
- [ ] Test GET /api/filters returns valid options
- [ ] Test POST /api/generate with various filter combinations
- [ ] Test empty result handling
- [ ] Test error scenarios

### Task 5.2: Test Web UI
- [ ] Test form loads and populates dropdowns
- [ ] Test report generation flow
- [ ] Test file download works
- [ ] Test on different browsers

### Task 5.3: Documentation
- [ ] Add usage instructions to README
- [ ] Document CloudFront URL location
- [ ] Add troubleshooting section

## Dependencies

```
Task 1.1 ──▶ Task 1.2 ──▶ Task 1.3
                              │
Task 2.1 ──▶ Task 2.2 ◀───────┘
    │
    ▼
Task 4.2 ◀── Task 4.1 ◀── Task 3.1 ──▶ Task 3.2
    │                         │
    │                         ▼
    │                     Task 3.3
    │                         │
    ▼                         ▼
Task 4.3 ◀────────────────────┘
    │
    ▼
Task 5.1 ──▶ Task 5.2 ──▶ Task 5.3
```

## Estimated Effort

| Phase | Tasks | Estimated Time |
|-------|-------|----------------|
| Phase 1 | Lambda API | 2-3 hours |
| Phase 2 | API Gateway | 1 hour |
| Phase 3 | Static Website | 2 hours |
| Phase 4 | CloudFront/S3 | 1-2 hours |
| Phase 5 | Testing/Docs | 1 hour |
| **Total** | | **7-9 hours** |

## Reference Files

- Existing Lambda: `#[[file:lambda/failure-analysis-report/main.py]]`
- CDK Stack: `#[[file:app.py]]`
- DynamoDB Table: `pdf-failure-analysis` (defined in app.py)
