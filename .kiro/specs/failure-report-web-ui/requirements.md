# Failure Report Web UI - Requirements

## Overview
Provide a web-based interface for team members to generate filtered PDF failure analysis reports on-demand, without requiring AWS CLI access.

## User Stories

### US-1: Generate Filtered Report
**As a** team member  
**I want to** generate a failure analysis report filtered by collection folder  
**So that** I can quickly see why PDFs in a specific batch failed

**Acceptance Criteria:**
- User can select a collection folder from a dropdown
- Report generates within 30 seconds
- Excel file downloads automatically to browser

### US-2: Filter by Error Type
**As a** team member  
**I want to** filter the report by error code  
**So that** I can focus on specific failure types (e.g., INTERNAL_SERVER_ERROR, TIMEOUT)

**Acceptance Criteria:**
- User can select one or more error codes
- User can select "All" to include all error types
- Filter is applied before report generation

### US-3: Filter by Date Range
**As a** team member  
**I want to** filter failures by date range  
**So that** I can see failures from a specific time period

**Acceptance Criteria:**
- User can specify start and end dates
- Defaults to last 7 days if not specified
- Date picker UI for easy selection

### US-4: Filter by API Type
**As a** team member  
**I want to** filter by API type (Autotag vs Extract)  
**So that** I can isolate failures to a specific processing stage

**Acceptance Criteria:**
- Radio buttons: Both, Autotag only, Extract only
- Defaults to "Both"

### US-5: View Available Filters
**As a** team member  
**I want to** see available collection folders and error codes in dropdowns  
**So that** I don't have to remember or type exact values

**Acceptance Criteria:**
- Collection folder dropdown populated from DynamoDB
- Error code dropdown shows codes that exist in the data
- Dropdowns refresh when page loads

## Functional Requirements

### FR-1: Web Form
- Single-page HTML form hosted on S3/CloudFront
- Responsive design (works on desktop and tablet)
- Clear status indicators (loading, success, error)

### FR-2: API Endpoints
- `GET /api/filters` - Returns available filter options
- `POST /api/generate` - Generates report with specified filters

### FR-3: Report Generation
- Reuse existing failure-analysis-report Lambda logic
- Add filter parameters to DynamoDB query
- Return presigned S3 URL for download (valid 1 hour)

### FR-4: Error Handling
- Display user-friendly error messages
- Handle Lambda timeout gracefully
- Show "No results found" if filters match nothing

## Non-Functional Requirements

### NFR-1: Performance
- Form loads in < 2 seconds
- Report generation completes in < 30 seconds for typical queries
- Filter options load in < 3 seconds

### NFR-2: Security
- HTTPS only (via CloudFront)
- No authentication required (internal tool)
- API Gateway throttling: 10 requests/second

### NFR-3: Cost
- Target: < $5/month for typical usage
- Use existing DynamoDB table (no additional storage)
- Leverage Lambda free tier where possible

### NFR-4: Availability
- Leverage AWS managed services (S3, CloudFront, API Gateway)
- No custom servers to maintain

## Out of Scope
- User authentication/login
- Saved/scheduled reports
- Email delivery of reports
- Real-time failure notifications
