Create an AWS Lambda function in Python that copies PDF files from a source S3 bucket to a destination S3 bucket.

Trigger:
	• The function is triggered by an S3 Event Notification when an object is created anywhere under the /result prefix in the source bucket.
	• Each invocation will process a single S3 event record (one file per invocation).
PDF Validation:
	• Check that the object key ends with .pdf (case-insensitive).
	• If the file is not a PDF, log a message indicating the file was skipped and return early without error.
Copy Behavior:
	• Copy the file from the source bucket to a destination bucket whose name is read from the DESTINATION_BUCKET environment variable.
	• Strip the leading /result prefix from the object key when constructing the destination key. For example, /result/folder1/folder2/filename.pdf in the source bucket becomes /folder1/folder2/filename.pdf in the destination bucket.
	• If a file with the same key already exists in the destination bucket, overwrite it.
Logging:
	• Log the source bucket and key.
	• Log the destination bucket and key.
	• Log a success message after a successful copy.
	• If the copy fails, catch the exception, log the error reason, and re-raise the exception so the Lambda invocation is marked as failed.
Lambda Configuration Guidance:
	• Keep memory and timeout as small as practical. The function only makes an S3 copy API call (server-side copy, no data passes through the function), so 128 MB memory and a 30-second timeout should be sufficient.
	• Use the Python 3.12 runtime.
	• Use arm64 architecture for cost savings.
Code Constraints:
	• Use only boto3 (included in the Lambda runtime). No additional dependencies.
	• Keep the code minimal and focused. No unnecessary abstractions.
