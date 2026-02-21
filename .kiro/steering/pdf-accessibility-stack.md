---
inclusion: auto
---

# PDFAccessibility Stack Guidelines

## Stack Information

This project uses AWS CDK with a single stack called `PDFAccessibility` defined in `app.py`.

## Critical Rules

- **ALL infrastructure updates MUST be made to the PDFAccessibility stack in `app.py`**
- Do not create additional CDK stacks unless explicitly requested
- All AWS resources (Lambda functions, ECS tasks, S3 buckets, Step Functions, etc.) should be added to the existing PDFAccessibility stack
- Maintain consistency with the existing stack structure and naming conventions

## Stack Structure

The PDFAccessibility stack includes:
- S3 bucket for PDF processing (`pdf_processing_bucket`)
- VPC with public and private subnets (`pdf_processing_vpc`)
- ECS cluster for PDF remediation tasks (`pdf_remediation_cluster`)
- Lambda functions for various processing steps
- Step Functions state machine for orchestration (`pdf_remediation_state_machine`)
- CloudWatch dashboard for monitoring
- EventBridge rules for scheduling

## When Adding New Resources

1. Add the resource definition within the `PDFAccessibility` class in `app.py`
2. Follow the existing naming convention (e.g., `resource_name_with_underscores`)
3. Use `self` to reference the stack context
4. Grant appropriate IAM permissions using the existing patterns
5. Add CloudWatch logging where appropriate
6. Update the dashboard if the resource needs monitoring
