# PDF Accessibility Remediation - Deployment Guide

This guide provides step-by-step instructions for deploying the PDF Accessibility Remediation system to AWS.

## Prerequisites

### Required Software
- **AWS CLI**: Configured with credentials for your target AWS account
- **AWS CDK**: `npm install -g aws-cdk`
- **Docker**: Required for building Lambda container images
- **Maven**: Required for building the Java PDF merger Lambda
- **Python 3.12+**: For CDK app execution
- **Node.js**: For CDK

### Fedora/RHEL Installation Commands
```bash
# Install Docker
sudo dnf install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
newgrp docker

# Install Maven
sudo dnf install maven

# Install AWS CDK
npm install -g aws-cdk

# Install Python dependencies
pip install -r requirements.txt
```

### AWS Prerequisites
- AWS account with appropriate permissions
- Adobe PDF Services API credentials stored in AWS Secrets Manager at `/myapp/client_credentials`
- Bedrock model access enabled (for Nova Pro and Claude Sonnet models)

## Deployment Methods

### Method 1: Local CDK Deployment (Recommended)

This method builds all Docker images locally and deploys directly, avoiding caching issues.

#### Step 1: Configure AWS Credentials
```bash
# Configure AWS CLI for your target account
aws configure

# Verify credentials
aws sts get-caller-identity
```

#### Step 2: Bootstrap CDK (First Time Only)
```bash
# Bootstrap CDK in your account/region
cdk bootstrap
```

#### Step 3: Build Java Lambda
```bash
# Navigate to Java project and build
cd lambda/pdf-merger-lambda/PDFMergerLambda
mvn clean package
cd ../../..
```

#### Step 4: Deploy Stack
```bash
# Deploy the stack
cdk deploy PDFAccessibility --require-approval never
```

#### Step 5: Verify Deployment
```bash
# Get the S3 bucket name
BUCKET_NAME=$(aws cloudformation describe-stack-resources \
  --stack-name PDFAccessibility \
  --query 'StackResources[?ResourceType==`AWS::S3::Bucket`].PhysicalResourceId' \
  --output text)

echo "Deployment complete! S3 Bucket: $BUCKET_NAME"

# Test with a sample PDF
aws s3 cp test.pdf s3://$BUCKET_NAME/pdf/test-folder/test.pdf
```

### Method 2: CodeBuild Deployment (Alternative)

This method uses AWS CodeBuild and CodePipeline for deployment.

**Note**: This method may experience Docker caching issues. Use Method 1 for more reliable deployments.

#### Step 1: Update deploy.sh
Edit `deploy.sh` and update:
- `GITHUB_URL`: Your GitHub repository URL
- `SOURCE_VERSION`: Your branch name (default: `usf-pdfa-one`)

#### Step 2: Run Deployment Script
```bash
./deploy.sh
```

## Post-Deployment Configuration

### 1. Configure Adobe API Credentials

Store your Adobe PDF Services credentials in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name /myapp/client_credentials \
  --secret-string '{
    "client_credentials": {
      "PDF_SERVICES_CLIENT_ID": "your-client-id",
      "PDF_SERVICES_CLIENT_SECRET": "your-client-secret"
    }
  }'
```

### 2. Verify Bedrock Access

Ensure your AWS account has access to:
- `us.amazon.nova-pro-v1:0` (for title generation)
- Claude Sonnet 3.5 models (for alt-text generation)

Request model access in the AWS Bedrock console if needed.

### 3. Test the Pipeline

Upload a test PDF to verify the complete workflow:

```bash
# Get bucket name
BUCKET_NAME=$(aws cloudformation describe-stack-resources \
  --stack-name PDFAccessibility \
  --query 'StackResources[?ResourceType==`AWS::S3::Bucket`].PhysicalResourceId' \
  --output text)

# Upload test PDF to a subfolder
aws s3 cp sample.pdf s3://$BUCKET_NAME/pdf/test-batch/sample.pdf

# Monitor processing (wait 2-3 minutes)
sleep 180

# Check results - folder structure should be preserved
aws s3 ls s3://$BUCKET_NAME/temp/test-batch/ --recursive
aws s3 ls s3://$BUCKET_NAME/result/test-batch/ --recursive
```

Expected output structure:
- `temp/test-batch/sample/` - Contains chunks and intermediate files
- `result/test-batch/COMPLIANT_sample.pdf` - Final accessible PDF

## Updating an Existing Deployment

### Update Code Changes

```bash
# 1. Make your code changes

# 2. If Java code changed, rebuild
cd lambda/pdf-merger-lambda/PDFMergerLambda
mvn clean package
cd ../../..

# 3. Commit changes
git add -A
git commit -m "Description of changes"
git push origin usf-pdfa-one

# 4. Redeploy
cdk deploy PDFAccessibility --require-approval never
```

### Update Only Lambda Functions

```bash
# For Python Lambdas (uses Docker)
cdk deploy PDFAccessibility --require-approval never

# For Java Lambda, rebuild first
cd lambda/pdf-merger-lambda/PDFMergerLambda
mvn clean package
cd ../../..
cdk deploy PDFAccessibility --require-approval never
```

## Monitoring and Troubleshooting

### View CloudWatch Dashboard

```bash
# Get dashboard URL
aws cloudwatch list-dashboards --query 'DashboardEntries[?contains(DashboardName, `PDF_Processing`)].DashboardName' --output text
```

Access the dashboard in AWS Console: CloudWatch → Dashboards

### View Lambda Logs

```bash
# PDF Splitter logs
aws logs tail /aws/lambda/$(aws lambda list-functions --query "Functions[?contains(FunctionName, 'PdfChunkSplitterLambda')].FunctionName" --output text) --follow

# PDF Merger logs
aws logs tail /aws/lambda/$(aws lambda list-functions --query "Functions[?contains(FunctionName, 'PdfMergerLambda')].FunctionName" --output text) --follow

# Title Generator logs
aws logs tail /aws/lambda/$(aws lambda list-functions --query "Functions[?contains(FunctionName, 'BedrockTitleGeneratorLambda')].FunctionName" --output text) --follow
```

### View Step Functions Execution

```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn $(aws stepfunctions list-state-machines --query 'stateMachines[?contains(name, `PdfAccessibilityRemediationWorkflow`)].stateMachineArn' --output text) \
  --max-results 10
```

### Common Issues

#### Issue: Docker permission denied
```bash
# Solution: Add user to docker group
sudo usermod -aG docker $USER
newgrp docker
```

#### Issue: Maven not found
```bash
# Solution: Install Maven
sudo dnf install maven
```

#### Issue: CDK bootstrap required
```bash
# Solution: Bootstrap CDK
cdk bootstrap
```

#### Issue: Folder structure not preserved
- Verify Java Lambda was rebuilt: `mvn clean package`
- Check merger logs for DEBUG output showing correct paths
- Ensure latest code is deployed (not cached)

## Cleanup

To remove the stack and all resources:

```bash
# Delete the stack
cdk destroy PDFAccessibility --force

# Note: S3 bucket may be retained if it contains files
# Manually empty and delete if needed:
BUCKET_NAME=$(aws cloudformation describe-stack-resources \
  --stack-name PDFAccessibility \
  --query 'StackResources[?ResourceType==`AWS::S3::Bucket`].PhysicalResourceId' \
  --output text 2>/dev/null)

if [ ! -z "$BUCKET_NAME" ]; then
  aws s3 rm s3://$BUCKET_NAME --recursive
  aws s3 rb s3://$BUCKET_NAME
fi
```

## Architecture Overview

The system processes PDFs through the following stages:

1. **Upload**: PDF uploaded to `s3://bucket/pdf/folder/file.pdf`
2. **Split**: Lambda splits into chunks → `temp/folder/filename/filename_chunk_N.pdf`
3. **Adobe Autotag**: ECS task adds accessibility tags
4. **Alt-Text Generation**: ECS task generates alt-text for images → `temp/folder/filename/FINAL_filename_chunk_N.pdf`
5. **Merge**: Lambda merges chunks → `temp/folder/filename/merged_filename.pdf`
6. **Title Generation**: Lambda generates accessible title
7. **Final Output**: Compliant PDF saved to `result/folder/COMPLIANT_filename.pdf`

Folder structure is preserved throughout the pipeline.

## Support

For issues or questions:
- Check CloudWatch logs for error details
- Review Step Functions execution history
- Consult `docs/TROUBLESHOOTING_CDK_DEPLOY.md` for deployment issues
- Review `docs/FIX_APPLIED_SUMMARY.md` for folder preservation implementation details
