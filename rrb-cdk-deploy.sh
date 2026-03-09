#!/bin/bash

# Function to play completion notification
notify_completion() {
    local status=$1
    local message=$2
    
    # Try text-to-speech first (spd-say is common on Linux)
    if command -v spd-say &> /dev/null; then
        spd-say "$message" 2>/dev/null &
    elif command -v espeak &> /dev/null; then
        espeak "$message" 2>/dev/null &
    elif command -v say &> /dev/null; then
        # macOS
        say "$message" 2>/dev/null &
    fi
    
    # Also try terminal bell as backup
    echo -e '\a'
    
    # Play a sound file if available (Linux)
    if [ "$status" = "success" ]; then
        # Try common success sound locations
        for sound in /usr/share/sounds/freedesktop/stereo/complete.oga \
                     /usr/share/sounds/gnome/default/alerts/glass.ogg \
                     /usr/share/sounds/ubuntu/stereo/message.ogg; do
            if [ -f "$sound" ]; then
                paplay "$sound" 2>/dev/null &
                break
            fi
        done
    else
        # Try common error sound locations
        for sound in /usr/share/sounds/freedesktop/stereo/dialog-error.oga \
                     /usr/share/sounds/gnome/default/alerts/bark.ogg; do
            if [ -f "$sound" ]; then
                paplay "$sound" 2>/dev/null &
                break
            fi
        done
    fi
}

echo "Checking AWS identity..."
echo ""
aws sts get-caller-identity
echo ""

read -p "Is this the correct AWS identity? [Y/n] " response
response=${response:-Y}

if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 1
fi

echo ""
deployment_type="${AWS_DEPLOYMENT_TYPE}"
echo "deployment_type=${deployment_type}"
read -p "press any key to continue"

echo ""
echo "Proceeding with ${deployment_type} deployment..."
echo "Cleaning CDK output directory..."
rm -rf ./cdk.out/

echo "Clearing Docker build cache to force image rebuild..."
docker builder prune -af 2>/dev/null || true
docker system prune -af 2>/dev/null || true

echo ""
echo "Starting CDK deployment with forced image rebuild..."
echo "Build timestamp will be embedded in Docker image to ensure new code is deployed."

echo "source_bucket=${AWS_PROJECT_S3_BUCKET_NAME}"
echo "destination_buket=${AWS_DESTINATION_BUCKET_NAME}"
read -p "Press any key to continue"

# Run CDK deploy and capture exit code
cdk deploy PDFAccessibility -c source_bucket=${AWS_PROJECT_S3_BUCKET_NAME} -c destination_bucket=${AWS_DESTINATION_BUCKET_NAME} --require-approval never --force
CDK_EXIT_CODE=$?

echo ""
echo "Deployment completed at: $(date '+%Y-%m-%d %H:%M:%S')"

# Notify based on success/failure
if [ $CDK_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ ${deployment_type} deployment SUCCEEDED!"
    notify_completion "success" "${deployment_type} deployment completed successfully"
else
    echo ""
    echo "❌ ${deployment_type} deployment FAILED with exit code $CDK_EXIT_CODE"
    notify_completion "failure" "${deployemnt_type} deployment failed"
    exit $CDK_EXIT_CODE
fi

echo ""
echo "IMPORTANT: After deployment, verify the new image is running:"
echo "1. Check ECR for new image with recent timestamp"
echo "2. Check ECS task definition points to new image URI"
echo "3. If tasks are still using old code, stop them in ECS console"
echo ""
echo "To verify rate limiter is working, check CloudWatch logs for:"
echo "  - 'Initial jitter:' messages (proves new code is running)"
echo "  - 'RPM (global)' messages (proves combined counter is active)"
echo ""

