#!/usr/bin/env python3
"""
CLI tool to manage the PDF failure digest Lambda and its schedule.

Usage:
    ./manage-digest.py trigger                    # Manually trigger the digest Lambda
    ./manage-digest.py trigger --force            # Reset notified flags and re-trigger
    ./manage-digest.py schedule                   # Show current schedule
    ./manage-digest.py schedule <HH:MM>           # Set schedule time (UTC)
    ./manage-digest.py schedule --reset           # Reset to default (23:55 UTC)
    ./manage-digest.py email                      # Show email feature status
    ./manage-digest.py email --enable             # Enable email notifications
    ./manage-digest.py email --disable            # Disable email (use S3 reports instead)

Examples:
    ./manage-digest.py trigger                    # Send digest now
    ./manage-digest.py trigger --force            # Reset and re-send
    ./manage-digest.py schedule 15:00             # Change to 3:00 PM UTC for testing
    ./manage-digest.py email --disable            # Switch to S3 reports
"""

import argparse
import boto3
import json
import sys
import re
from botocore.exceptions import ClientError

LAMBDA_FUNCTION_NAME = "pdf-failure-digest-handler"
EVENTBRIDGE_RULE_NAME = "pdf-failure-digest-daily"
FAILURE_TABLE_NAME = "pdf-failure-records"
EMAIL_ENABLED_PARAM = "/pdf-processing/email-enabled"
SENDER_EMAIL_PARAM = "/pdf-processing/sender-email"
DEFAULT_SCHEDULE_TIME = "23:55"


def get_lambda_client():
    return boto3.client('lambda')


def get_events_client():
    return boto3.client('events')


def get_dynamodb_resource():
    return boto3.resource('dynamodb')


def get_ssm_client():
    return boto3.client('ssm')


def show_email_status():
    """Show current email feature status."""
    ssm = get_ssm_client()
    
    print("Email Feature Status:")
    print("-" * 40)
    
    # Check email enabled
    try:
        response = ssm.get_parameter(Name=EMAIL_ENABLED_PARAM)
        enabled = response['Parameter']['Value'].lower() == 'true'
        print(f"  Email enabled: {'Yes' if enabled else 'No (using S3 reports)'}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            print(f"  Email enabled: No (parameter not set, defaulting to S3 reports)")
        else:
            print(f"  Email enabled: Error - {e.response['Error']['Message']}")
    
    # Check sender email
    try:
        response = ssm.get_parameter(Name=SENDER_EMAIL_PARAM)
        print(f"  Sender email: {response['Parameter']['Value']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            print(f"  Sender email: Not configured")
        else:
            print(f"  Sender email: Error - {e.response['Error']['Message']}")
    
    print()
    print("When email is disabled, reports are saved to:")
    print("  s3://bucket/reports/deletion_reports/{username}/{username}-{timestamp}.txt")
    
    return True


def set_email_enabled(enabled: bool) -> bool:
    """Enable or disable email feature."""
    ssm = get_ssm_client()
    
    try:
        ssm.put_parameter(
            Name=EMAIL_ENABLED_PARAM,
            Value='true' if enabled else 'false',
            Type='String',
            Overwrite=True
        )
        
        if enabled:
            print("✓ Email notifications enabled")
            print("  Make sure sender email is configured:")
            print(f"  aws ssm put-parameter --name \"{SENDER_EMAIL_PARAM}\" --value \"your-email@domain.com\" --type String --overwrite")
        else:
            print("✓ Email notifications disabled")
            print("  Reports will be saved to S3: reports/deletion_reports/{username}/")
        
        return True
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Message']}", file=sys.stderr)
        return False


def reset_todays_notifications():
    """Reset all notified flags for today's failures so they can be re-sent."""
    from datetime import datetime, timezone
    
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(FAILURE_TABLE_NAME)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    print(f"Resetting notified flags for failures on {today}...")
    
    try:
        # Query for today's failures using the GSI
        response = table.query(
            IndexName='failure_date-index',
            KeyConditionExpression='failure_date = :date',
            ExpressionAttributeValues={':date': today}
        )
        
        items = response.get('Items', [])
        
        if not items:
            print(f"  No failures found for {today}")
            return 0
        
        # Reset notified flag for each item
        reset_count = 0
        for item in items:
            failure_id = item.get('failure_id')
            if failure_id:
                table.update_item(
                    Key={'failure_id': failure_id},
                    UpdateExpression='SET notified = :n',
                    ExpressionAttributeValues={':n': False}
                )
                reset_count += 1
        
        print(f"  ✓ Reset {reset_count} failure records")
        return reset_count
        
    except Exception as e:
        print(f"  ✗ Error resetting notifications: {e}")
        return 0


def trigger_digest():
    """Manually trigger the digest Lambda function."""
    lambda_client = get_lambda_client()
    
    print(f"Triggering {LAMBDA_FUNCTION_NAME}...")
    
    try:
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        # Read the raw response
        raw_payload = response['Payload'].read().decode('utf-8')
        status_code = response.get('StatusCode', 0)
        function_error = response.get('FunctionError', None)
        
        # Check for Lambda execution errors
        if function_error:
            print(f"✗ Lambda execution error: {function_error}")
            print(f"  Raw response: {raw_payload}")
            return False
        
        # Try to parse JSON response
        if raw_payload and raw_payload.strip():
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                print(f"✓ Lambda executed (status {status_code})")
                print(f"  Raw response: {raw_payload}")
                return status_code == 200
        else:
            print(f"✓ Lambda executed (status {status_code})")
            print(f"  No response body returned")
            return status_code == 200
        
        if status_code == 200:
            print(f"✓ Lambda executed successfully")
            if 'body' in payload:
                try:
                    body = json.loads(payload['body']) if isinstance(payload['body'], str) else payload['body']
                    print(f"  - Emails sent: {body.get('emails_sent', 'N/A')}")
                    print(f"  - S3 reports generated: {body.get('reports_generated', 'N/A')}")
                    print(f"  - Failures processed: {body.get('failures_processed', 'N/A')}")
                    print(f"  - Users processed: {body.get('users_processed', 'N/A')}")
                except (json.JSONDecodeError, TypeError):
                    print(f"  Body: {payload['body']}")
            elif 'statusCode' in payload:
                print(f"  Status: {payload.get('statusCode')}")
                if 'body' in payload:
                    print(f"  Body: {payload.get('body')}")
            else:
                print(f"  Response: {payload}")
            return True
        else:
            print(f"✗ Lambda returned status code: {status_code}")
            print(f"  Response: {payload}")
            return False
            
    except ClientError as e:
        print(f"✗ Error invoking Lambda: {e.response['Error']['Message']}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        print(f"  Type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False


def get_current_schedule():
    """Get the current schedule from EventBridge rule."""
    events_client = get_events_client()
    
    try:
        response = events_client.describe_rule(Name=EVENTBRIDGE_RULE_NAME)
        schedule_expression = response.get('ScheduleExpression', '')
        state = response.get('State', 'UNKNOWN')
        
        # Parse cron expression: cron(55 23 * * ? *)
        match = re.search(r'cron\((\d+)\s+(\d+)\s+', schedule_expression)
        if match:
            minute = match.group(1).zfill(2)
            hour = match.group(2).zfill(2)
            return f"{hour}:{minute}", state, schedule_expression
        
        return None, state, schedule_expression
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return None, 'NOT_FOUND', None
        raise


def show_schedule():
    """Display the current schedule."""
    time_str, state, raw_expression = get_current_schedule()
    
    if time_str is None:
        if state == 'NOT_FOUND':
            print(f"✗ EventBridge rule '{EVENTBRIDGE_RULE_NAME}' not found")
            print("  Has the stack been deployed?")
        else:
            print(f"? Could not parse schedule: {raw_expression}")
        return False
    
    print(f"Current digest schedule:")
    print(f"  - Time: {time_str} UTC")
    print(f"  - State: {state}")
    print(f"  - Rule: {EVENTBRIDGE_RULE_NAME}")
    print(f"  - Expression: {raw_expression}")
    return True


def set_schedule(time_str: str):
    """Set a new schedule time."""
    events_client = get_events_client()
    
    # Parse time string (HH:MM)
    match = re.match(r'^(\d{1,2}):(\d{2})$', time_str)
    if not match:
        print(f"✗ Invalid time format: {time_str}", file=sys.stderr)
        print("  Use HH:MM format (e.g., 15:00 for 3:00 PM UTC)")
        return False
    
    hour = int(match.group(1))
    minute = int(match.group(2))
    
    if hour < 0 or hour > 23:
        print(f"✗ Invalid hour: {hour}. Must be 0-23.", file=sys.stderr)
        return False
    
    if minute < 0 or minute > 59:
        print(f"✗ Invalid minute: {minute}. Must be 0-59.", file=sys.stderr)
        return False
    
    # Build cron expression: cron(minute hour * * ? *)
    cron_expression = f"cron({minute} {hour} * * ? *)"
    
    try:
        # Get current rule to preserve other settings
        current = events_client.describe_rule(Name=EVENTBRIDGE_RULE_NAME)
        
        # Update the rule
        events_client.put_rule(
            Name=EVENTBRIDGE_RULE_NAME,
            ScheduleExpression=cron_expression,
            State=current.get('State', 'ENABLED'),
            Description=current.get('Description', 'Daily digest of PDF processing failures')
        )
        
        print(f"✓ Schedule updated to {hour:02d}:{minute:02d} UTC")
        print(f"  Expression: {cron_expression}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"✗ EventBridge rule '{EVENTBRIDGE_RULE_NAME}' not found", file=sys.stderr)
            print("  Has the stack been deployed?")
        else:
            print(f"✗ Error updating schedule: {e.response['Error']['Message']}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Manage the PDF failure digest Lambda and schedule",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s trigger                    # Send digest emails now
  %(prog)s schedule                   # Show current schedule
  %(prog)s schedule 15:00             # Change to 3:00 PM UTC
  %(prog)s schedule 23:55             # Change to 11:55 PM UTC
  %(prog)s schedule --reset           # Reset to default (23:55 UTC)

Note: All times are in UTC.
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Trigger command
    trigger_parser = subparsers.add_parser('trigger', help='Manually trigger the digest Lambda')
    trigger_parser.add_argument('--force', action='store_true', 
                                help='Reset all notified flags for today and re-send emails')
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='View or set the digest schedule')
    schedule_parser.add_argument('time', nargs='?', help='New schedule time in HH:MM format (UTC)')
    schedule_parser.add_argument('--reset', action='store_true', help='Reset to default schedule (23:55 UTC)')
    
    # Email command
    email_parser = subparsers.add_parser('email', help='View or toggle email feature')
    email_group = email_parser.add_mutually_exclusive_group()
    email_group.add_argument('--enable', action='store_true', help='Enable email notifications')
    email_group.add_argument('--disable', action='store_true', help='Disable email (use S3 reports)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    success = False
    
    if args.command == 'trigger':
        if args.force:
            reset_todays_notifications()
        success = trigger_digest()
    
    elif args.command == 'schedule':
        if args.reset:
            success = set_schedule(DEFAULT_SCHEDULE_TIME)
        elif args.time:
            success = set_schedule(args.time)
        else:
            success = show_schedule()
    
    elif args.command == 'email':
        if args.enable:
            success = set_email_enabled(True)
        elif args.disable:
            success = set_email_enabled(False)
        else:
            success = show_email_status()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
