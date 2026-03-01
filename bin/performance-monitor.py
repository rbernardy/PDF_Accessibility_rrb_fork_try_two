#!/usr/bin/env python3
"""
PDF Processing Performance Monitor

Monitors the performance of the PDF processing system and provides
recommendations for SSM parameter tuning.

Usage: python bin/performance-monitor.py [--interval SECONDS] [--duration MINUTES]
"""

import argparse
import boto3
import json
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# AWS clients
cloudwatch = boto3.client('cloudwatch')
dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')
ecs = boto3.client('ecs')
stepfunctions = boto3.client('stepfunctions')
logs = boto3.client('logs')

# Constants
RATE_LIMIT_TABLE = 'adobe-api-in-flight-tracker'
IN_FLIGHT_COUNTER_ID = 'adobe_api_in_flight'
GLOBAL_BACKOFF_ID = 'global_backoff_until'


class PerformanceMonitor:
    def __init__(self):
        self.metrics_history = defaultdict(list)
        self.alerts = []
        self.recommendations = []
        
    def get_ssm_parameters(self):
        """Fetch all PDF processing SSM parameters."""
        params = {}
        try:
            response = ssm.get_parameters_by_path(
                Path='/pdf-processing/',
                Recursive=True
            )
            for param in response.get('Parameters', []):
                name = param['Name'].split('/')[-1]
                params[name] = param['Value']
        except Exception as e:
            print(f"Error fetching SSM parameters: {e}")
        return params
    
    def get_in_flight_status(self):
        """Get current in-flight counter and file count."""
        try:
            table = dynamodb.Table(RATE_LIMIT_TABLE)
            
            # Get counter value
            counter_response = table.get_item(Key={'counter_id': IN_FLIGHT_COUNTER_ID})
            counter_value = int(counter_response.get('Item', {}).get('in_flight', 0))
            
            # Count actual file entries
            scan_response = table.scan(
                FilterExpression='begins_with(counter_id, :prefix) AND attribute_not_exists(released)',
                ExpressionAttributeValues={':prefix': 'file_'},
                Select='COUNT'
            )
            file_count = scan_response.get('Count', 0)
            
            # Check for global backoff
            backoff_response = table.get_item(Key={'counter_id': GLOBAL_BACKOFF_ID})
            backoff_item = backoff_response.get('Item')
            backoff_remaining = 0
            if backoff_item:
                backoff_until = int(backoff_item.get('backoff_until', 0))
                backoff_remaining = max(0, backoff_until - int(time.time()))
            
            return {
                'counter_value': counter_value,
                'file_count': file_count,
                'counter_drift': counter_value - file_count,
                'backoff_remaining': backoff_remaining
            }
        except Exception as e:
            print(f"Error getting in-flight status: {e}")
            return None
    def get_in_flight_files(self):
        """Get list of files currently being processed from DynamoDB tracker."""
        try:
            table = dynamodb.Table(RATE_LIMIT_TABLE)
            response = table.scan(
                FilterExpression='begins_with(counter_id, :prefix) AND attribute_not_exists(released)',
                ExpressionAttributeValues={':prefix': 'file_'}
            )
            files = []
            for item in response.get('Items', []):
                counter_id = item.get('counter_id', '')
                # Extract filename from counter_id (format: file_<s3_key>)
                if counter_id.startswith('file_'):
                    s3_key = counter_id[5:]  # Remove 'file_' prefix
                    filename = s3_key.split('/')[-1] if '/' in s3_key else s3_key
                    files.append({
                        'counter_id': counter_id,
                        's3_key': s3_key,
                        'filename': filename,
                        'timestamp': item.get('timestamp', item.get('created_at', 0))
                    })
            return files
        except Exception as e:
            print(f"Error getting in-flight files: {e}")
            return []

    def get_step_function_metrics(self):
        """Get Step Function execution metrics."""
        try:
            # Find the state machine
            response = stepfunctions.list_state_machines()
            state_machine_arn = None
            for sm in response.get('stateMachines', []):
                if 'PdfAccessibilityRemediationWorkflow' in sm['name']:
                    state_machine_arn = sm['stateMachineArn']
                    break
            
            if not state_machine_arn:
                return None
            
            # Count executions by status
            running = 0
            succeeded_recent = 0
            failed_recent = 0
            
            # Running executions
            running_response = stepfunctions.list_executions(
                stateMachineArn=state_machine_arn,
                statusFilter='RUNNING',
                maxResults=100
            )
            running = len(running_response.get('executions', []))
            
            # Recent succeeded (last 10 minutes)
            # The stopDate from boto3 is already timezone-aware
            ten_min_ago = datetime.now(timezone.utc) - timedelta(minutes=10)
            
            succeeded_response = stepfunctions.list_executions(
                stateMachineArn=state_machine_arn,
                statusFilter='SUCCEEDED',
                maxResults=100
            )
            for ex in succeeded_response.get('executions', []):
                stop_date = ex.get('stopDate')
                if stop_date:
                    # boto3 returns timezone-aware datetime, compare directly
                    if stop_date > ten_min_ago:
                        succeeded_recent += 1
            
            # Recent failed (last 10 minutes)
            failed_response = stepfunctions.list_executions(
                stateMachineArn=state_machine_arn,
                statusFilter='FAILED',
                maxResults=100
            )
            for ex in failed_response.get('executions', []):
                stop_date = ex.get('stopDate')
                if stop_date:
                    if stop_date > ten_min_ago:
                        failed_recent += 1
            
            return {
                'running': running,
                'succeeded_10min': succeeded_recent,
                'failed_10min': failed_recent,
                'state_machine_arn': state_machine_arn
            }
        except Exception as e:
            print(f"Error getting Step Function metrics: {e}")
            return None
    
    def get_ecs_metrics(self):
        """Get ECS cluster and task metrics."""
        try:
            # Find the cluster
            clusters_response = ecs.list_clusters()
            cluster_arn = None
            for arn in clusters_response.get('clusterArns', []):
                if 'PdfRemediation' in arn:
                    cluster_arn = arn
                    break
            
            if not cluster_arn:
                return None
            
            # Get running tasks
            tasks_response = ecs.list_tasks(
                cluster=cluster_arn,
                desiredStatus='RUNNING'
            )
            running_tasks = len(tasks_response.get('taskArns', []))
            
            # Get task details if any running
            task_details = []
            if tasks_response.get('taskArns'):
                describe_response = ecs.describe_tasks(
                    cluster=cluster_arn,
                    tasks=tasks_response['taskArns'][:20]  # Limit to 20
                )
                for task in describe_response.get('tasks', []):
                    started_at = task.get('startedAt')
                    duration = None
                    if started_at:
                        duration = (datetime.now(timezone.utc) - started_at).total_seconds()
                    
                    # Extract container info and environment variables to find filename
                    filename = None
                    container_name = None
                    for container in task.get('containers', []):
                        container_name = container.get('name', 'unknown')
                    
                    # Try to get filename from task overrides (environment variables)
                    overrides = task.get('overrides', {})
                    for container_override in overrides.get('containerOverrides', []):
                        for env in container_override.get('environment', []):
                            if env.get('name') in ['INPUT_KEY', 'S3_KEY', 'PDF_KEY', 'FILENAME']:
                                filename = env.get('value', '').split('/')[-1]
                                break
                    
                    task_details.append({
                        'task_arn': task['taskArn'].split('/')[-1],
                        'task_id': task['taskArn'].split('/')[-1][:12],
                        'container_name': container_name,
                        'status': task.get('lastStatus'),
                        'duration_seconds': duration,
                        'filename': filename
                    })
            
            return {
                'cluster_arn': cluster_arn,
                'running_tasks': running_tasks,
                'task_details': task_details
            }
        except Exception as e:
            print(f"Error getting ECS metrics: {e}")
            return None
    
    def get_cloudwatch_metrics(self, minutes=10):
        """Get relevant CloudWatch metrics."""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=minutes)
        
        metrics = {}
        
        try:
            # First, get actual Lambda function names by listing functions
            lambda_client = boto3.client('lambda')
            functions_response = lambda_client.list_functions(MaxItems=50)
            all_functions = {f['FunctionName']: f['FunctionName'] for f in functions_response.get('Functions', [])}
            
            # Map friendly names to actual function names (search by pattern)
            function_patterns = {
                'pdf-splitter': ['PdfChunkSplitter', 'pdf-splitter', 'Splitter'],
                'pdf-merger': ['PdfMerger', 'pdf-merger', 'Merger'],
                'title-generator': ['TitleGenerator', 'title-generator', 'BedrockTitle'],
                'queue-processor': ['pdf-queue-processor', 'QueueProcessor', 'retry-processor']
            }
            
            lambda_functions = {}
            for friendly_name, patterns in function_patterns.items():
                for func_name in all_functions:
                    for pattern in patterns:
                        if pattern.lower() in func_name.lower():
                            lambda_functions[friendly_name] = func_name
                            break
                    if friendly_name in lambda_functions:
                        break
            
            for friendly_name, func_name in lambda_functions.items():
                try:
                    # Invocations
                    inv_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/Lambda',
                        MetricName='Invocations',
                        Dimensions=[{'Name': 'FunctionName', 'Value': func_name}],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,
                        Statistics=['Sum']
                    )
                    invocations = sum(dp['Sum'] for dp in inv_response.get('Datapoints', []))
                    
                    # Errors
                    err_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/Lambda',
                        MetricName='Errors',
                        Dimensions=[{'Name': 'FunctionName', 'Value': func_name}],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,
                        Statistics=['Sum']
                    )
                    errors = sum(dp['Sum'] for dp in err_response.get('Datapoints', []))
                    
                    metrics[friendly_name] = {
                        'invocations': int(invocations),
                        'errors': int(errors)
                    }
                except:
                    pass
            
        except Exception as e:
            print(f"Error getting CloudWatch metrics: {e}")
        
        return metrics
    
    def check_for_429_errors(self, minutes=30):
        """Check CloudWatch logs for 429 rate limit errors."""
        try:
            log_group = '/ecs/adobe-autotag-processor'
            end_time = int(time.time() * 1000)
            start_time = end_time - (minutes * 60 * 1000)
            
            response = logs.filter_log_events(
                logGroupName=log_group,
                startTime=start_time,
                endTime=end_time,
                filterPattern='429',
                limit=100
            )
            
            return len(response.get('events', []))
        except Exception as e:
            # Log group might not exist
            return 0
    
    def analyze_and_recommend(self, params, in_flight, step_funcs, ecs_metrics, cw_metrics, error_429_count):
        """Analyze metrics and generate recommendations."""
        self.recommendations = []
        self.alerts = []
        
        # Get current parameter values
        max_in_flight = int(params.get('adobe-api-max-in-flight', 150))
        adobe_rpm = int(params.get('adobe-api-rpm', 200))
        queue_batch_size = int(params.get('queue-batch-size', 8))
        queue_max_executions = int(params.get('queue-max-executions', 75))
        max_retries = int(params.get('max-retries', 3))
        
        # Analysis 1: In-flight utilization
        if in_flight:
            utilization = (in_flight['counter_value'] / max_in_flight * 100) if max_in_flight > 0 else 0
            
            if utilization < 30 and step_funcs and step_funcs['running'] > 0:
                self.recommendations.append({
                    'category': 'In-Flight Slots',
                    'severity': 'info',
                    'message': f"Low in-flight utilization ({utilization:.1f}%). Consider increasing queue-batch-size from {queue_batch_size} to {queue_batch_size + 4} to process more files concurrently."
                })
            elif utilization > 90:
                self.recommendations.append({
                    'category': 'In-Flight Slots',
                    'severity': 'warning',
                    'message': f"High in-flight utilization ({utilization:.1f}%). System is near capacity. If no 429 errors, consider increasing adobe-api-max-in-flight from {max_in_flight}."
                })
            
            # Counter drift check
            if in_flight['counter_drift'] > 5:
                self.alerts.append({
                    'severity': 'warning',
                    'message': f"Counter drift detected: counter={in_flight['counter_value']}, actual files={in_flight['file_count']}. May indicate crashed containers."
                })
            
            # Backoff check
            if in_flight['backoff_remaining'] > 0:
                self.alerts.append({
                    'severity': 'critical',
                    'message': f"Global backoff active ({in_flight['backoff_remaining']}s remaining). Adobe API rate limit was hit!"
                })
        
        # Analysis 2: 429 errors
        if error_429_count > 0:
            self.alerts.append({
                'severity': 'critical',
                'message': f"Found {error_429_count} rate limit (429) errors in the last 30 minutes!"
            })
            self.recommendations.append({
                'category': 'Rate Limiting',
                'severity': 'critical',
                'message': f"Reduce adobe-api-max-in-flight from {max_in_flight} to {max(50, max_in_flight - 30)} to avoid 429 errors."
            })
        elif in_flight and in_flight['counter_value'] > 0:
            # No 429s and actively processing - might be able to increase
            if max_in_flight < 180:
                self.recommendations.append({
                    'category': 'Rate Limiting',
                    'severity': 'info',
                    'message': f"No 429 errors detected. If throughput is needed, consider gradually increasing adobe-api-max-in-flight from {max_in_flight} to {min(180, max_in_flight + 20)}."
                })
        
        # Analysis 3: Step Function executions
        if step_funcs:
            if step_funcs['failed_10min'] > 0:
                failure_rate = step_funcs['failed_10min'] / max(1, step_funcs['failed_10min'] + step_funcs['succeeded_10min']) * 100
                if failure_rate > 20:
                    self.alerts.append({
                        'severity': 'warning',
                        'message': f"High Step Function failure rate: {failure_rate:.1f}% in last 10 minutes."
                    })
            
            if step_funcs['running'] > queue_max_executions * 0.9:
                self.recommendations.append({
                    'category': 'Queue Processing',
                    'severity': 'warning',
                    'message': f"Running executions ({step_funcs['running']}) near max ({queue_max_executions}). Consider increasing queue-max-executions if system is stable."
                })
        
        # Analysis 4: ECS task duration
        if ecs_metrics and ecs_metrics['task_details']:
            long_running = [t for t in ecs_metrics['task_details'] if t['duration_seconds'] and t['duration_seconds'] > 1800]
            if long_running:
                task_list = []
                for t in long_running:
                    duration_min = int(t['duration_seconds'] / 60)
                    filename = t.get('filename') or 'unknown'
                    task_list.append(f"{t['task_id']} ({filename}, {duration_min}m)")
                self.alerts.append({
                    'severity': 'warning',
                    'message': f"{len(long_running)} ECS tasks running > 30 minutes: {', '.join(task_list)}"
                })
        
        # Analysis 5: Lambda errors
        total_errors = sum(m.get('errors', 0) for m in cw_metrics.values())
        if total_errors > 10:
            self.alerts.append({
                'severity': 'warning',
                'message': f"{total_errors} Lambda errors in the last 10 minutes. Check CloudWatch logs."
            })
    
    def print_report(self, params, in_flight, step_funcs, ecs_metrics, cw_metrics, error_429_count):
        """Print the performance report."""
        print("=" * 70)
        print("PDF PROCESSING PERFORMANCE REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Current SSM Parameters
        print("--- Current SSM Parameters ---")
        important_params = [
            'adobe-api-max-in-flight',
            'adobe-api-rpm', 
            'queue-batch-size',
            'queue-batch-size-low-load',
            'queue-max-in-flight',
            'queue-max-executions',
            'max-retries',
            'queue-enabled'
        ]
        for param in important_params:
            value = params.get(param, 'not set')
            print(f"  {param}: {value}")
        
        # In-Flight Status
        print("--- In-Flight Status ---")
        if in_flight:
            max_in_flight = int(params.get('adobe-api-max-in-flight', 150))
            utilization = (in_flight['counter_value'] / max_in_flight * 100) if max_in_flight > 0 else 0
            print(f"  Counter value: {in_flight['counter_value']}")
            print(f"  Actual files: {in_flight['file_count']}")
            print(f"  Utilization: {utilization:.1f}% of {max_in_flight} max")
            if in_flight['counter_drift'] != 0:
                print(f"  Counter drift: {in_flight['counter_drift']}")
            if in_flight['backoff_remaining'] > 0:
                print(f"  ⚠️  Global backoff: {in_flight['backoff_remaining']}s remaining")
        else:
            print("  Unable to fetch in-flight status")
        
        # Step Functions
        print("--- Step Function Status ---")
        if step_funcs:
            print(f"  Running executions: {step_funcs['running']}")
            print(f"  Succeeded (10min): {step_funcs['succeeded_10min']}")
            print(f"  Failed (10min): {step_funcs['failed_10min']}")
        else:
            print("  Unable to fetch Step Function status")
        
        # ECS Status
        print("--- ECS Status ---")
        if ecs_metrics:
            print(f"  Running tasks: {ecs_metrics['running_tasks']}")
            if ecs_metrics['task_details']:
                for task in ecs_metrics['task_details'][:10]:
                    duration = f"{int(task['duration_seconds'] / 60)}m" if task['duration_seconds'] else '?'
                    container = task.get('container_name') or 'unknown'
                    filename = task.get('filename') or 'unknown'
                    task_id = task.get('task_id', task['task_arn'][:12])
                    warning = " ⚠️" if task['duration_seconds'] and task['duration_seconds'] > 1800 else ""
                    print(f"    - {task_id} | {container} | {filename} | {duration}{warning}")
            
            # Show in-flight files from DynamoDB
            in_flight_files = self.get_in_flight_files()
            if in_flight_files:
                print(f"\n  Files being processed (from DynamoDB):")
                for f in in_flight_files[:15]:  # Limit to 15
                    print(f"    - {f['filename']}")
                if len(in_flight_files) > 15:
                    print(f"    ... and {len(in_flight_files) - 15} more")
        else:
            print("  Unable to fetch ECS status")
        
        # Lambda Metrics
        print("--- Lambda Metrics (10min) ---")
        if cw_metrics:
            for func, metrics in cw_metrics.items():
                error_indicator = " ⚠️" if metrics['errors'] > 0 else ""
                print(f"  {func}: {metrics['invocations']} invocations, {metrics['errors']} errors{error_indicator}")
        else:
            print("  No Lambda metrics available")
        
        # 429 Errors
        print("--- Rate Limit Errors (30min) ---")
        if error_429_count > 0:
            print(f"  ⚠️  429 errors found: {error_429_count}")
        else:
            print("  No 429 errors detected ✓")
        
        # Alerts
        if self.alerts:
            print("=" * 70)
            print("⚠️  ALERTS")
            print("=" * 70)
            for alert in self.alerts:
                severity_icon = "🔴" if alert['severity'] == 'critical' else "🟡"
                print(f"  {severity_icon} [{alert['severity'].upper()}] {alert['message']}")
        
        # Recommendations
        if self.recommendations:
            print("=" * 70)
            print("📋 RECOMMENDATIONS")
            print("=" * 70)
            for rec in self.recommendations:
                severity_icon = "🔴" if rec['severity'] == 'critical' else "🟡" if rec['severity'] == 'warning' else "ℹ️"
                print(f"  {severity_icon} [{rec['category']}]")
                print(f"     {rec['message']}")
        
        if not self.alerts and not self.recommendations:
            print("=" * 70)
            print("✅ System appears to be running optimally")
            print("=" * 70)
    
    def run_once(self):
        """Run a single performance check."""
        params = self.get_ssm_parameters()
        in_flight = self.get_in_flight_status()
        step_funcs = self.get_step_function_metrics()
        ecs_metrics = self.get_ecs_metrics()
        cw_metrics = self.get_cloudwatch_metrics()
        error_429_count = self.check_for_429_errors()
        
        self.analyze_and_recommend(params, in_flight, step_funcs, ecs_metrics, cw_metrics, error_429_count)
        self.print_report(params, in_flight, step_funcs, ecs_metrics, cw_metrics, error_429_count)
    
    def run_continuous(self, interval_seconds=60, duration_minutes=None):
        """Run continuous monitoring."""
        start_time = time.time()
        iteration = 0
        
        print(f"Starting continuous monitoring (interval: {interval_seconds}s)")
        if duration_minutes:
            print(f"Will run for {duration_minutes} minutes")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                iteration += 1
                self.run_once()
                
                # Check duration limit
                if duration_minutes:
                    elapsed = (time.time() - start_time) / 60
                    if elapsed >= duration_minutes:
                        print(f"\nDuration limit reached ({duration_minutes} minutes)")
                        break
                
                next_run = datetime.now() + timedelta(seconds=interval_seconds)
                print(f"Next check at {next_run.strftime('%Y-%m-%d %H:%M:%S')} (Ctrl+C to stop)")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped by user")


def main():
    parser = argparse.ArgumentParser(
        description='Monitor PDF processing performance and get tuning recommendations'
    )
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=60,
        help='Interval between checks in seconds (default: 60)'
    )
    parser.add_argument(
        '--duration', '-d',
        type=int,
        default=None,
        help='Total duration to run in minutes (default: run until Ctrl+C)'
    )
    parser.add_argument(
        '--once', '-1',
        action='store_true',
        help='Run once and exit'
    )
    
    args = parser.parse_args()
    
    monitor = PerformanceMonitor()
    
    if args.once:
        monitor.run_once()
    else:
        monitor.run_continuous(
            interval_seconds=args.interval,
            duration_minutes=args.duration
        )


if __name__ == '__main__':
    main()
