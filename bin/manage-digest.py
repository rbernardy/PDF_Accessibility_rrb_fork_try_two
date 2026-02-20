#!/usr/bin/env python3
"""
CLI tool to manage the PDF failure digest Lambda and its schedule.

Usage:
    ./manage-digest.py trigger                    # Manually trigger the digest Lambda
    ./manage-digest.py schedule                   # Show current schedule
    ./manage-digest.py schedule <HH:MM>           # Set schedule time (UTC)
    ./manage-digest.py schedule --reset           # Reset to default (23:55 UTC)

Examples:
    ./manage-digest.py trigger                    # Send digest emails now
    ./manage-digest.py schedule                   # Show when digest runs
    ./manage-digest.py schedule 15:00             # Change to 3:00 PM UTC for testing
    ./manage-digest.py schedule 23:55             # Change to 11:55 PM UTC
    ./manage-digest.py schedule --reset           # Reset to default 11:55 PM UTC
"""

import argparse
import boto3
import json
import sys
import re
from botocore.exceptions import ClientError

LAMBDA_FUNCTION_NAME = "pdf-failure-digest-handler"
EVENTBRIDGE_RULE_NAME = "pdf-failure-digest-daily"
DEFAULT_SCHEDULE_TIME = "23:55"


def get_lambda_client():
    return boto3.client('lambda')


def get_events_client():
    return boto3.client('events')


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
        
        # Read the response
        payload = json.loads(response['Payload'].read().decode('utf-8'))
        status_code = response.get('StatusCode', 0)
        
        if status_code == 200:
            print(f"✓ Lambda executed successfully")
            if 'body' in payload:
                body = json.loads(payload['body']) if isinstance(payload['body'], str) else payload['body']
                print(f"  - Emails sent: {body.get('emails_sent', 'N/A')}")
                print(f"  - Failures processed: {body.get('failures_processed', 'N/A')}")
                print(f"  - Users processed: {body.get('users_processed', 'N/A')}")
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
    subparsers.add_parser('trigger', help='Manually trigger the digest Lambda')
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='View or set the digest schedule')
    schedule_parser.add_argument('time', nargs='?', help='New schedule time in HH:MM format (UTC)')
    schedule_parser.add_argument('--reset', action='store_true', help='Reset to default schedule (23:55 UTC)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    success = False
    
    if args.command == 'trigger':
        success = trigger_digest()
    
    elif args.command == 'schedule':
        if args.reset:
            success = set_schedule(DEFAULT_SCHEDULE_TIME)
        elif args.time:
            success = set_schedule(args.time)
        else:
            success = show_schedule()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
