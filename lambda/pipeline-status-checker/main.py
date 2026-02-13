"""
Pipeline Status Checker Lambda

This Lambda function checks the PDF processing pipeline status every minute.
It monitors Step Function executions and ECS tasks to determine if the system
is actively processing or idle.

Outputs:
- CloudWatch custom metric for pipeline status
- CloudWatch Logs entry with status details
"""

import os
import json
import boto3
from datetime import datetime, timezone
from typing import Dict, Any, List


# Initialize clients
sfn_client = boto3.client('stepfunctions')
ecs_client = boto3.client('ecs')
cloudwatch_client = boto3.client('cloudwatch')


def get_running_executions(state_machine_arn: str) -> List[Dict]:
    """
    Get all running Step Function executions.
    
    Args:
        state_machine_arn: ARN of the state machine
        
    Returns:
        List of running execution details
    """
    running_executions = []
    
    try:
        paginator = sfn_client.get_paginator('list_executions')
        for page in paginator.paginate(
            stateMachineArn=state_machine_arn,
            statusFilter='RUNNING'
        ):
            for execution in page.get('executions', []):
                running_executions.append({
                    'executionArn': execution['executionArn'],
                    'name': execution['name'],
                    'startDate': execution['startDate'].isoformat()
                })
    except Exception as e:
        print(f"Error listing executions: {e}")
    
    return running_executions


def get_running_ecs_tasks(cluster_name: str) -> int:
    """
    Get count of running ECS tasks in the cluster.
    
    Args:
        cluster_name: Name of the ECS cluster
        
    Returns:
        Number of running tasks
    """
    try:
        response = ecs_client.list_tasks(
            cluster=cluster_name,
            desiredStatus='RUNNING'
        )
        return len(response.get('taskArns', []))
    except Exception as e:
        print(f"Error listing ECS tasks: {e}")
        return 0


def publish_metrics(is_processing: bool, execution_count: int, task_count: int) -> None:
    """
    Publish custom CloudWatch metrics.
    
    Args:
        is_processing: Whether the pipeline is actively processing
        execution_count: Number of running Step Function executions
        task_count: Number of running ECS tasks
    """
    timestamp = datetime.now(timezone.utc)
    
    try:
        cloudwatch_client.put_metric_data(
            Namespace='PDFAccessibility/Pipeline',
            MetricData=[
                {
                    'MetricName': 'PipelineStatus',
                    'Value': 1 if is_processing else 0,
                    'Timestamp': timestamp,
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'Status', 'Value': 'Processing' if is_processing else 'Idle'}
                    ]
                },
                {
                    'MetricName': 'RunningExecutions',
                    'Value': execution_count,
                    'Timestamp': timestamp,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'RunningECSTasks',
                    'Value': task_count,
                    'Timestamp': timestamp,
                    'Unit': 'Count'
                }
            ]
        )
    except Exception as e:
        print(f"Error publishing metrics: {e}")


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Lambda handler for checking pipeline status.
    
    Args:
        event: Lambda event
        context: Lambda context
        
    Returns:
        Status response
    """
    # Get configuration from environment
    state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
    cluster_name = os.environ.get('ECS_CLUSTER_NAME')
    
    if not state_machine_arn:
        return {
            'statusCode': 400,
            'body': 'Missing STATE_MACHINE_ARN environment variable'
        }
    
    # Get current timestamp
    check_time = datetime.now(timezone.utc)
    
    # Check Step Function executions
    running_executions = get_running_executions(state_machine_arn)
    execution_count = len(running_executions)
    
    # Check ECS tasks if cluster name is provided
    task_count = 0
    if cluster_name:
        task_count = get_running_ecs_tasks(cluster_name)
    
    # Determine if pipeline is processing
    is_processing = execution_count > 0 or task_count > 0
    status = "Processing" if is_processing else "Idle"
    
    # Publish CloudWatch metrics
    publish_metrics(is_processing, execution_count, task_count)
    
    # Build status message for logging (this will appear in CloudWatch Logs)
    status_entry = {
        'timestamp': check_time.isoformat(),
        'status': status,
        'running_executions': execution_count,
        'running_ecs_tasks': task_count,
        'execution_details': running_executions[:5] if running_executions else []
    }
    
    # Log the status entry (this is what the dashboard will query)
    print(f"PIPELINE_STATUS: {json.dumps(status_entry)}")
    
    return {
        'statusCode': 200,
        'body': status_entry
    }
