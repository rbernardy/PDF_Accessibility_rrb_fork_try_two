#!/usr/bin/env python3
"""
In-Flight Tracker Cleanup Tool

Removes file entries from the DynamoDB in-flight tracker table based on
a filename substring match. Useful for cleaning up stuck or orphaned entries.

Usage:
    # Preview what would be deleted (dry-run)
    python bin/cleanup-in-flight.py --pattern "tobacco_leaf"
    
    # Actually delete matching entries
    python bin/cleanup-in-flight.py --pattern "tobacco_leaf" --remove
"""

import argparse
import boto3
from botocore.exceptions import ClientError

# Default table name
DEFAULT_TABLE = 'adobe-api-in-flight-tracker'
COUNTER_ID = 'adobe_api_in_flight'


def get_dynamodb_table(table_name):
    """Get DynamoDB table resource."""
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(table_name)


def scan_file_entries(table):
    """Scan for all entries where counter_id starts with 'file_'."""
    file_entries = []
    
    # Scan with filter for file_ prefix
    scan_kwargs = {
        'FilterExpression': 'begins_with(counter_id, :prefix)',
        'ExpressionAttributeValues': {':prefix': 'file_'}
    }
    
    while True:
        response = table.scan(**scan_kwargs)
        file_entries.extend(response.get('Items', []))
        
        # Handle pagination
        if 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break
    
    return file_entries


def filter_by_pattern(entries, pattern):
    """Filter entries where counter_id contains the pattern."""
    return [e for e in entries if pattern in e.get('counter_id', '')]


def delete_entries(table, entries):
    """Delete the specified entries from the table."""
    deleted_count = 0
    errors = []
    
    for entry in entries:
        counter_id = entry.get('counter_id')
        try:
            table.delete_item(Key={'counter_id': counter_id})
            deleted_count += 1
        except ClientError as e:
            errors.append(f"Failed to delete {counter_id}: {e.response['Error']['Message']}")
    
    return deleted_count, errors


def decrement_counter(table, amount):
    """Decrement the in-flight counter by the specified amount."""
    if amount <= 0:
        return True, None
    
    try:
        table.update_item(
            Key={'counter_id': COUNTER_ID},
            UpdateExpression='SET current_count = current_count - :dec',
            ExpressionAttributeValues={':dec': amount},
            ConditionExpression='attribute_exists(counter_id)'
        )
        return True, None
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return False, f"Counter '{COUNTER_ID}' does not exist"
        return False, e.response['Error']['Message']


def get_current_counter(table):
    """Get the current value of the in-flight counter."""
    try:
        response = table.get_item(Key={'counter_id': COUNTER_ID})
        item = response.get('Item')
        if item:
            return item.get('current_count', 0)
        return None
    except ClientError:
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Clean up file entries from the DynamoDB in-flight tracker table'
    )
    parser.add_argument(
        '--pattern', '-p',
        required=True,
        help='Substring to match in the filename/s3_key (required)'
    )
    parser.add_argument(
        '--remove',
        action='store_true',
        help='Actually delete the matching entries (default is dry-run mode)'
    )
    parser.add_argument(
        '--table',
        default=DEFAULT_TABLE,
        help=f'DynamoDB table name (default: {DEFAULT_TABLE})'
    )
    
    args = parser.parse_args()
    
    print(f"Scanning DynamoDB table: {args.table}")
    print(f'Looking for files matching: "{args.pattern}"')
    print()
    
    # Get table reference
    table = get_dynamodb_table(args.table)
    
    # Get current counter value
    current_count = get_current_counter(table)
    if current_count is not None:
        print(f"Current in-flight counter: {current_count}")
        print()
    
    # Scan for file entries
    print("Scanning for file entries...")
    all_file_entries = scan_file_entries(table)
    print(f"Found {len(all_file_entries)} total file entries in table")
    
    # Filter by pattern
    matching_entries = filter_by_pattern(all_file_entries, args.pattern)
    
    if not matching_entries:
        print(f'\nNo file entries found matching "{args.pattern}"')
        return 0
    
    # Display matching entries
    print(f"\nFound {len(matching_entries)} matching file entries:")
    for i, entry in enumerate(matching_entries, 1):
        counter_id = entry.get('counter_id', 'unknown')
        print(f"  {i}. {counter_id}")
    
    print()
    
    if args.remove:
        # Actually delete the entries
        print(f"Deleting {len(matching_entries)} entries...")
        deleted_count, errors = delete_entries(table, matching_entries)
        
        if errors:
            print("\nErrors during deletion:")
            for error in errors:
                print(f"  - {error}")
        
        print(f"\nDeleted {deleted_count} file entries")
        
        # Decrement the counter
        if deleted_count > 0:
            print(f"Decrementing in-flight counter by {deleted_count}...")
            success, error = decrement_counter(table, deleted_count)
            if success:
                new_count = get_current_counter(table)
                if new_count is not None:
                    print(f"Counter updated: {current_count} -> {new_count}")
                else:
                    print("Counter decremented successfully")
            else:
                print(f"Warning: Failed to decrement counter: {error}")
        
        print("\nCleanup complete.")
    else:
        # Dry-run mode
        print("DRY RUN - No changes made. Use --remove to delete these entries.")
        if current_count is not None:
            new_count = max(0, current_count - len(matching_entries))
            print(f"Counter would be updated: {current_count} -> {new_count}")
    
    return 0


if __name__ == '__main__':
    exit(main())
