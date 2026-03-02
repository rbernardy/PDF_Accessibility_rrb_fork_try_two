#!/usr/bin/env python3
"""
Step Function Failure Diagnosis Tool

Analyzes recent Step Function failures to identify patterns and root causes
in the PDF processing pipeline. Can fetch CloudWatch logs from ECS containers
for detailed error context.

Usage: 
  python bin/diagnose-failures.py [--count N] [--hours N] [--verbose] [--logs]

Examples:
  python bin/diagnose-failures.py                    # Basic summary of last 10 failures
  python bin/diagnose-failures.py -c 5 -H 4         # Last 5 failures from past 4 hours
  python bin/diagnose-failures.py --logs            # Include CloudWatch container logs
  python bin/diagnose-failures.py -v                # Full verbose output with logs
"""

import argparse
import boto3
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# AWS clients
stepfunctions = boto3.client('stepfunctions')
logs = boto3.client('logs')


# Container log group configurations
CONTAINER_LOG_CONFIGS = {
    'adobe-autotag-container': {
        'log_group': '/ecs/pdf-remediation/adobe-autotag',
        'stream_prefix': 'AdobeAutotagLogs'
    },
    'alt-text-llm-container': {
        'log_group': '/ecs/pdf-remediation/alt-text-generator',
        'stream_prefix': 'AltTextGeneratorLogs'
    }
}

# Debug flag - set via --debug argument
DEBUG_MODE = False


def get_container_error_logs(task_arn, container_name='adobe-autotag-container', log_group=None):
    """Fetch the last error/exception logs from a container's CloudWatch logs."""
    if not task_arn:
        return None
    
    # Extract task ID from ARN
    # ARN format: arn:aws:ecs:region:account:task/cluster-name/task-id
    task_id = task_arn.split('/')[-1] if '/' in task_arn else task_arn
    
    # Get log config for this container
    config = CONTAINER_LOG_CONFIGS.get(container_name, {})
    if not log_group:
        log_group = config.get('log_group', '/ecs/pdf-remediation/adobe-autotag')
    stream_prefix = config.get('stream_prefix', 'AdobeAutotagLogs')
    
    # Try multiple log stream name patterns
    # Pattern 1: {prefix}/{container-name}/{task-id} (standard ECS awslogs pattern)
    # Pattern 2: {prefix}/{task-id}
    # Pattern 3: Just search by task-id
    patterns_to_try = [
        f"{stream_prefix}/{container_name}/{task_id}",
        f"{stream_prefix}/{task_id}",
        task_id,  # Just the task ID as prefix
    ]
    
    if DEBUG_MODE:
        print(f"     [DEBUG] Looking for logs in {log_group}")
        print(f"     [DEBUG] Task ARN: {task_arn}")
        print(f"     [DEBUG] Task ID: {task_id}")
    
    for pattern in patterns_to_try:
        try:
            if DEBUG_MODE:
                print(f"     [DEBUG] Trying stream prefix: {pattern}")
            
            streams_response = logs.describe_log_streams(
                logGroupName=log_group,
                logStreamNamePrefix=pattern,
                limit=5
            )
            
            streams = streams_response.get('logStreams', [])
            
            if DEBUG_MODE and streams:
                print(f"     [DEBUG] Found {len(streams)} stream(s): {[s['logStreamName'] for s in streams]}")
            
            if not streams:
                continue
            
            # Use the first matching stream
            log_stream_name = streams[0]['logStreamName']
            
            # Fetch the last N log events
            events_response = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream_name,
                limit=50,
                startFromHead=False  # Get most recent
            )
            
            events = events_response.get('events', [])
            
            if DEBUG_MODE:
                print(f"     [DEBUG] Found {len(events)} log events")
            
            # Look for error patterns in the logs (search from end)
            error_lines = []
            for event in reversed(events):
                message = event.get('message', '')
                msg_lower = message.lower()
                
                # Look for error indicators
                if any(pattern in msg_lower for pattern in ['error', 'exception', 'traceback', 'failed', 'fatal', 'critical']):
                    error_lines.append(message.strip())
                    # Collect up to 5 error-related lines
                    if len(error_lines) >= 5:
                        break
            
            if error_lines:
                # Return errors in chronological order
                return '\n'.join(reversed(error_lines))
            
            # If no explicit errors found, return the last few lines
            last_lines = [e.get('message', '').strip() for e in events[-5:]]
            return '\n'.join(last_lines) if last_lines else None
            
        except logs.exceptions.ResourceNotFoundException:
            if DEBUG_MODE:
                print(f"     [DEBUG] Log group {log_group} not found")
            continue
        except Exception as e:
            if DEBUG_MODE:
                print(f"     [DEBUG] Error with pattern {pattern}: {str(e)}")
            continue
    
    # If we get here, try listing recent streams to help debug
    if DEBUG_MODE:
        try:
            recent_streams = logs.describe_log_streams(
                logGroupName=log_group,
                orderBy='LastEventTime',
                descending=True,
                limit=5
            )
            streams = recent_streams.get('logStreams', [])
            if streams:
                print(f"     [DEBUG] Recent streams in {log_group}:")
                for s in streams:
                    print(f"     [DEBUG]   - {s['logStreamName']}")
        except Exception as e:
            print(f"     [DEBUG] Could not list recent streams: {e}")
    
    return None


def get_all_container_logs_for_task(task_arn, failed_step=None):
    """
    Fetch logs from all relevant containers for a task.
    Returns a dict of container_name -> log_content.
    """
    if not task_arn:
        return {}
    
    results = {}
    
    # Determine which containers to check based on failed step
    containers_to_check = list(CONTAINER_LOG_CONFIGS.keys())
    
    # If we know the failed step, prioritize that container
    if failed_step:
        step_lower = failed_step.lower()
        if 'autotag' in step_lower or 'adobe' in step_lower:
            containers_to_check = ['adobe-autotag-container']
        elif 'alt' in step_lower or 'text' in step_lower:
            containers_to_check = ['alt-text-llm-container']
    
    for container_name in containers_to_check:
        logs_content = get_container_error_logs(task_arn, container_name)
        if logs_content and 'Could not fetch logs' not in logs_content:
            results[container_name] = logs_content
    
    return results


def find_state_machine():
    """Find the PDF processing state machine ARN."""
    paginator = stepfunctions.get_paginator('list_state_machines')
    for page in paginator.paginate():
        for sm in page.get('stateMachines', []):
            if 'PdfAccessibilityRemediationWorkflow' in sm['name']:
                return sm['stateMachineArn'], sm['name']
    return None, None


def get_failed_executions(state_machine_arn, count, hours):
    """Get recent failed executions within the time window."""
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    failed_executions = []
    
    paginator = stepfunctions.get_paginator('list_executions')
    for page in paginator.paginate(
        stateMachineArn=state_machine_arn,
        statusFilter='FAILED'
    ):
        for execution in page.get('executions', []):
            stop_date = execution.get('stopDate')
            if stop_date and stop_date >= cutoff_time:
                failed_executions.append(execution)
            elif stop_date and stop_date < cutoff_time:
                # Executions are returned in reverse chronological order
                # Once we hit one older than cutoff, we can stop
                break
        
        if len(failed_executions) >= count:
            break
        
        # Check if we've gone past the time window
        if page.get('executions') and page['executions'][-1].get('stopDate'):
            if page['executions'][-1]['stopDate'] < cutoff_time:
                break
    
    return failed_executions[:count]


def extract_filename_from_input(input_data):
    """Try to extract the PDF filename from execution input."""
    if not input_data:
        return None
    
    try:
        data = json.loads(input_data) if isinstance(input_data, str) else input_data
        
        # Common patterns for finding the filename
        # Check direct keys
        for key in ['original_pdf_key', 'filename', 'fileName', 'file_name', 'key', 's3Key', 's3_key', 'inputKey', 'input_key']:
            if key in data:
                value = data[key]
                if isinstance(value, str):
                    # Extract just the filename from a path
                    return value.split('/')[-1]
        
        # Check nested structures
        if 'detail' in data:
            return extract_filename_from_input(data['detail'])
        if 'input' in data:
            return extract_filename_from_input(data['input'])
        if 'object' in data and 'key' in data['object']:
            return data['object']['key'].split('/')[-1]
        if 's3' in data and 'object' in data['s3']:
            return data['s3']['object'].get('key', '').split('/')[-1]
            
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    
    return None


def analyze_execution_history(execution_arn):
    """Analyze execution history to find the failed step and error details."""
    result = {
        'failed_step': None,
        'error_type': None,
        'error_message': None,
        'cause': None,
        'filename': None
    }
    
    try:
        # Get execution details for input
        exec_response = stepfunctions.describe_execution(executionArn=execution_arn)
        input_data = exec_response.get('input')
        result['filename'] = extract_filename_from_input(input_data)
        
        # Get execution history
        history_response = stepfunctions.get_execution_history(
            executionArn=execution_arn,
            reverseOrder=True,
            maxResults=100
        )
        
        events = history_response.get('events', [])
        
        # Look for failure events
        for event in events:
            event_type = event.get('type', '')
            
            # Task failures
            if event_type == 'TaskFailed':
                details = event.get('taskFailedEventDetails', {})
                result['failed_step'] = find_step_name_for_event(events, event.get('id'))
                result['error_type'] = details.get('error', 'Unknown')
                result['cause'] = details.get('cause', '')
                result['error_message'] = parse_error_cause(details.get('cause', ''))
                break
            
            # Lambda failures
            elif event_type == 'LambdaFunctionFailed':
                details = event.get('lambdaFunctionFailedEventDetails', {})
                result['failed_step'] = find_step_name_for_event(events, event.get('id'))
                result['error_type'] = details.get('error', 'Unknown')
                result['cause'] = details.get('cause', '')
                result['error_message'] = parse_error_cause(details.get('cause', ''))
                break
            
            # Activity failures
            elif event_type == 'ActivityFailed':
                details = event.get('activityFailedEventDetails', {})
                result['failed_step'] = find_step_name_for_event(events, event.get('id'))
                result['error_type'] = details.get('error', 'Unknown')
                result['cause'] = details.get('cause', '')
                result['error_message'] = parse_error_cause(details.get('cause', ''))
                break
            
            # Execution failures (catch-all)
            elif event_type == 'ExecutionFailed':
                details = event.get('executionFailedEventDetails', {})
                result['error_type'] = details.get('error', 'Unknown')
                result['cause'] = details.get('cause', '')
                result['error_message'] = parse_error_cause(details.get('cause', ''))
                # Continue looking for the actual failed step
            
            # Task timed out
            elif event_type == 'TaskTimedOut':
                details = event.get('taskTimedOutEventDetails', {})
                result['failed_step'] = find_step_name_for_event(events, event.get('id'))
                result['error_type'] = 'States.Timeout'
                result['error_message'] = 'Task timed out'
                result['cause'] = details.get('cause', '')
                break
        
        # Try to extract task ARN from ECS failure cause
        if result['cause']:
            try:
                cause_json = json.loads(result['cause'])
                if 'TaskArn' in cause_json:
                    result['task_arn'] = cause_json['TaskArn']
                # Also try Containers array
                elif 'Containers' in cause_json and cause_json['Containers']:
                    result['task_arn'] = cause_json['Containers'][0].get('TaskArn')
            except:
                pass
                
    except Exception as e:
        result['error_message'] = f"Error analyzing execution: {str(e)}"
    
    return result


def find_step_name_for_event(events, event_id):
    """Find the step/state name associated with an event."""
    # Look backwards through events to find the state that was entered
    for event in events:
        if event.get('type') == 'TaskStateEntered':
            details = event.get('stateEnteredEventDetails', {})
            return details.get('name')
        elif event.get('type') == 'LambdaFunctionScheduled':
            # Check previous event for state name
            prev_id = event.get('previousEventId')
            for e in events:
                if e.get('id') == prev_id and e.get('type') == 'TaskStateEntered':
                    return e.get('stateEnteredEventDetails', {}).get('name')
    return None


def parse_error_cause(cause):
    """Parse the error cause to extract a readable message."""
    if not cause:
        return None
    
    # Try to parse as JSON
    try:
        cause_data = json.loads(cause)
        
        # Check if this is ECS task failure JSON (has Containers array)
        if 'Containers' in cause_data and isinstance(cause_data['Containers'], list):
            container = cause_data['Containers'][0] if cause_data['Containers'] else {}
            exit_code = container.get('ExitCode', 'unknown')
            container_name = container.get('Name', 'unknown')
            stop_reason = cause_data.get('StoppedReason', '')
            stop_code = cause_data.get('StopCode', '')
            
            # Try to get the file being processed from environment overrides
            file_key = None
            overrides = cause_data.get('Overrides', {})
            for container_override in overrides.get('ContainerOverrides', []):
                for env in container_override.get('Environment', []):
                    if env.get('Name') in ['S3_FILE_KEY', 'S3_CHUNK_KEY', 'INPUT_KEY']:
                        file_key = env.get('Value', '').split('/')[-1]
                        break
                if file_key:
                    break
            
            # Build readable message
            msg_parts = [f"Exit code {exit_code}"]
            if stop_code:
                msg_parts.append(f"({stop_code})")
            if file_key:
                msg_parts.append(f"- processing {file_key}")
            
            return ' '.join(msg_parts)
        
        # Common patterns for other error types
        if 'errorMessage' in cause_data:
            return cause_data['errorMessage']
        if 'message' in cause_data:
            return cause_data['message']
        if 'Error' in cause_data:
            return cause_data['Error']
        if 'error' in cause_data:
            return cause_data['error']
            
    except (json.JSONDecodeError, TypeError):
        pass
    
    # Return truncated raw cause if not JSON
    if len(cause) > 200:
        return cause[:200] + '...'
    return cause


def categorize_error(error_type, error_message, cause):
    """Categorize the error into a human-readable group."""
    combined = f"{error_type or ''} {error_message or ''} {cause or ''}".lower()
    
    # Check for specific exit codes first
    if 'exit code 137' in combined:
        return 'Out of memory (OOM killed)'
    if 'exit code 139' in combined:
        return 'Segmentation fault (crash)'
    if 'exit code 143' in combined:
        return 'SIGTERM (timeout/shutdown)'
    if 'exit code 1' in combined:
        return 'Container error (exit code 1)'
    if 'exit code 0' in combined:
        return 'Container exited normally'
    
    if 'timeout' in combined or 'timed out' in combined:
        return 'Task timed out'
    if '429' in combined or 'rate limit' in combined or 'too many requests' in combined:
        return 'Rate limit exceeded (429)'
    if 'encrypted' in combined or 'password' in combined:
        return 'PDF is encrypted/password protected'
    if 'essentialcontainerexited' in combined:
        return 'Container crashed'
    if 'memory' in combined or 'oom' in combined:
        return 'Out of memory'
    if 'permission' in combined or 'access denied' in combined:
        return 'Permission denied'
    if 'not found' in combined or '404' in combined:
        return 'Resource not found'
    if 'invalid' in combined or 'malformed' in combined:
        return 'Invalid input'
    if 'connection' in combined or 'network' in combined:
        return 'Network/connection error'
    
    return error_type or 'Unknown error'


def print_report(failures, verbose=False, cause_length=None, show_logs=False):
    """Print the failure diagnosis report."""
    print("=" * 60)
    print("Step Function Failure Diagnosis")
    print("=" * 60)
    
    if not failures:
        print("\nNo failures found in the specified time window.")
        return
    
    # Aggregate by step
    step_counts = defaultdict(int)
    error_counts = defaultdict(int)
    
    for failure in failures:
        step = failure['analysis'].get('failed_step') or 'Unknown'
        step_counts[step] += 1
        
        error_category = categorize_error(
            failure['analysis'].get('error_type'),
            failure['analysis'].get('error_message'),
            failure['analysis'].get('cause')
        )
        error_counts[error_category] += 1
    
    # Print summary by step
    print("\nFAILURE SUMMARY BY STEP:")
    for step, count in sorted(step_counts.items(), key=lambda x: -x[1]):
        plural = 'failure' if count == 1 else 'failures'
        print(f"  {step}: {count} {plural}")
    
    # Print summary by error
    print("\nFAILURE SUMMARY BY ERROR:")
    for error, count in sorted(error_counts.items(), key=lambda x: -x[1]):
        plural = 'occurrence' if count == 1 else 'occurrences'
        print(f"  \"{error}\": {count} {plural}")
    
    # Print individual failures
    print("\nRECENT FAILURES:")
    for i, failure in enumerate(failures, 1):
        exec_id = failure['execution_arn'].split(':')[-1]
        stop_time = failure['stop_date'].strftime('%Y-%m-%d %H:%M:%S')
        step = failure['analysis'].get('failed_step') or 'Unknown'
        filename = failure['analysis'].get('filename') or 'unknown'
        error_msg = failure['analysis'].get('error_message') or failure['analysis'].get('error_type') or 'Unknown error'
        task_arn = failure['analysis'].get('task_arn')
        
        print(f"\n  {i}. {exec_id} | {stop_time} | {step}")
        print(f"     File: {filename}")
        print(f"     Error: {error_msg}")
        
        # Show task ID if available (useful for log correlation)
        if task_arn:
            task_id = task_arn.split('/')[-1] if '/' in task_arn else task_arn
            print(f"     Task ID: {task_id}")
        
        if verbose:
            error_type = failure['analysis'].get('error_type')
            cause = failure['analysis'].get('cause')
            if error_type:
                print(f"     Error Type: {error_type}")
            if cause:
                if cause_length and len(cause) > cause_length:
                    print(f"     Full Cause: {cause[:cause_length]}...")
                else:
                    print(f"     Full Cause: {cause}")
        
        # Fetch CloudWatch logs if requested (via --logs or --verbose)
        if show_logs:
            if task_arn:
                container_logs = get_all_container_logs_for_task(task_arn, step)
                if container_logs:
                    for container_name, logs_content in container_logs.items():
                        # Shorten container name for display
                        short_name = container_name.replace('-container', '')
                        print(f"     [{short_name}] CloudWatch Logs:")
                        for line in logs_content.split('\n')[:10]:
                            print(f"       {line[:150]}")
                else:
                    print(f"     CloudWatch Logs: (no logs found - streams may have expired or task didn't write logs)")
            else:
                # No task ARN - this is likely a Lambda failure, not ECS
                print(f"     CloudWatch Logs: (no ECS task ARN - this may be a Lambda failure, check Lambda logs)")


def main():
    parser = argparse.ArgumentParser(
        description='Diagnose Step Function failures in the PDF processing pipeline'
    )
    parser.add_argument(
        '--count', '-c',
        type=int,
        default=10,
        help='Number of recent failures to analyze (default: 10)'
    )
    parser.add_argument(
        '--hours', '-H',
        type=int,
        default=24,
        help='Only look at failures from the last N hours (default: 24)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show full error details for each failure'
    )
    parser.add_argument(
        '--logs', '-L',
        action='store_true',
        help='Fetch and show CloudWatch container logs for each failure'
    )
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Show debug info about log stream discovery (useful for troubleshooting)'
    )
    parser.add_argument(
        '--cause-length', '-l',
        type=int,
        default=None,
        help='Limit Full Cause output to first N characters (default: show all)'
    )
    
    args = parser.parse_args()
    
    # Set debug mode globally
    global DEBUG_MODE
    DEBUG_MODE = args.debug
    
    print(f"Analyzing last {args.count} failures from the past {args.hours} hours...\n")
    
    # Find the state machine
    state_machine_arn, state_machine_name = find_state_machine()
    if not state_machine_arn:
        print("ERROR: Could not find PdfAccessibilityRemediationWorkflow state machine.")
        print("Make sure you have the correct AWS credentials and region configured.")
        return 1
    
    print(f"Found state machine: {state_machine_name}")
    
    # Get failed executions
    failed_executions = get_failed_executions(state_machine_arn, args.count, args.hours)
    
    if not failed_executions:
        print(f"\nNo failed executions found in the last {args.hours} hours.")
        return 0
    
    print(f"Found {len(failed_executions)} failed execution(s)")
    
    # Analyze each failure
    failures = []
    for execution in failed_executions:
        analysis = analyze_execution_history(execution['executionArn'])
        failures.append({
            'execution_arn': execution['executionArn'],
            'stop_date': execution['stopDate'],
            'analysis': analysis
        })
    
    # Print report - show logs if --logs or --verbose
    print_report(failures, verbose=args.verbose, cause_length=args.cause_length, show_logs=args.logs or args.verbose)
    
    return 0


if __name__ == '__main__':
    exit(main())
