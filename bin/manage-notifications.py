#!/usr/bin/env python3
"""
CLI tool to manage PDF cleanup email notifications.

Usage:
    ./manage-notifications.py add <iam_username> <email>
    ./manage-notifications.py remove <iam_username>
    ./manage-notifications.py enable <iam_username>
    ./manage-notifications.py disable <iam_username>
    ./manage-notifications.py list

Examples:
    ./manage-notifications.py add jane.doe jane.doe@company.com
    ./manage-notifications.py remove john.smith
    ./manage-notifications.py list
"""

import argparse
import boto3
import sys
from datetime import datetime
from botocore.exceptions import ClientError

TABLE_NAME = "pdf-cleanup-notifications"


def get_dynamodb_table():
    """Get DynamoDB table resource."""
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(TABLE_NAME)


def add_user(username: str, email: str) -> bool:
    """Add or update a user's notification email."""
    table = get_dynamodb_table()
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    try:
        # Check if user exists
        response = table.get_item(Key={'iam_username': username})
        exists = 'Item' in response
        
        table.put_item(
            Item={
                'iam_username': username,
                'email': email,
                'enabled': True,
                'created_at': response['Item']['created_at'] if exists else timestamp,
                'updated_at': timestamp
            }
        )
        
        action = "Updated" if exists else "Added"
        print(f"✓ {action} notification for {username} -> {email}")
        return True
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Message']}", file=sys.stderr)
        return False


def remove_user(username: str) -> bool:
    """Remove a user from notifications."""
    table = get_dynamodb_table()
    
    try:
        # Check if user exists first
        response = table.get_item(Key={'iam_username': username})
        if 'Item' not in response:
            print(f"✗ User '{username}' not found", file=sys.stderr)
            return False
        
        table.delete_item(Key={'iam_username': username})
        print(f"✓ Removed notification for {username}")
        return True
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Message']}", file=sys.stderr)
        return False


def set_enabled(username: str, enabled: bool) -> bool:
    """Enable or disable notifications for a user."""
    table = get_dynamodb_table()
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    try:
        response = table.update_item(
            Key={'iam_username': username},
            UpdateExpression='SET enabled = :enabled, updated_at = :updated_at',
            ConditionExpression='attribute_exists(iam_username)',
            ExpressionAttributeValues={
                ':enabled': enabled,
                ':updated_at': timestamp
            },
            ReturnValues='ALL_NEW'
        )
        
        status = "Enabled" if enabled else "Disabled"
        print(f"✓ {status} notifications for {username}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f"✗ User '{username}' not found", file=sys.stderr)
        else:
            print(f"✗ Error: {e.response['Error']['Message']}", file=sys.stderr)
        return False


def list_users() -> bool:
    """List all configured users."""
    table = get_dynamodb_table()
    
    try:
        response = table.scan()
        items = response.get('Items', [])
        
        if not items:
            print("No users configured for notifications.")
            return True
        
        # Print header
        print(f"\n{'IAM Username':<25} {'Email':<35} {'Enabled':<10} {'Updated'}")
        print("-" * 90)
        
        # Sort by username
        for item in sorted(items, key=lambda x: x['iam_username']):
            username = item['iam_username']
            email = item.get('email', 'N/A')
            enabled = "Yes" if item.get('enabled', False) else "No"
            updated = item.get('updated_at', 'N/A')[:19]  # Trim to datetime
            
            print(f"{username:<25} {email:<35} {enabled:<10} {updated}")
        
        print(f"\nTotal: {len(items)} user(s)")
        return True
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Message']}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Manage PDF cleanup email notifications",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s add jane.doe jane.doe@company.com
  %(prog)s remove john.smith
  %(prog)s disable jane.doe
  %(prog)s enable jane.doe
  %(prog)s list
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add or update a user notification')
    add_parser.add_argument('username', help='IAM username')
    add_parser.add_argument('email', help='Email address for notifications')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a user from notifications')
    remove_parser.add_argument('username', help='IAM username')
    
    # Enable command
    enable_parser = subparsers.add_parser('enable', help='Enable notifications for a user')
    enable_parser.add_argument('username', help='IAM username')
    
    # Disable command
    disable_parser = subparsers.add_parser('disable', help='Disable notifications for a user')
    disable_parser.add_argument('username', help='IAM username')
    
    # List command
    subparsers.add_parser('list', help='List all configured users')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    success = False
    
    if args.command == 'add':
        success = add_user(args.username, args.email)
    elif args.command == 'remove':
        success = remove_user(args.username)
    elif args.command == 'enable':
        success = set_enabled(args.username, True)
    elif args.command == 'disable':
        success = set_enabled(args.username, False)
    elif args.command == 'list':
        success = list_users()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
