# PDF Failure Analysis Lambda
# Uses PyMuPDF for PDF structure analysis
# Optionally includes Poppler tools for additional validation

FROM public.ecr.aws/lambda/python:3.12

# Install Poppler tools (optional - uncomment if needed)
# RUN dnf install -y poppler-utils && dnf clean all

# Copy requirements and install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy function code
COPY main.py ${LAMBDA_TASK_ROOT}/
COPY analyzer.py ${LAMBDA_TASK_ROOT}/

# Set the handler
CMD ["main.lambda_handler"]
