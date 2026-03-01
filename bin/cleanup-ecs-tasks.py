#!/usr/bin/env python3
"""
ECS Task Cleanup Script
Stops ECS tasks that have been running longer than a specified threshold.
"""

import boto3
from datetime import datetime, timezone
import argparse
from collections import defaultdict


def get_ecs_clusters():
    """Get all ECS cluster ARNs."""
    ecs = boto3.client('ecs')
    clusters = []
    paginator = ecs.get_paginator('list_clusters')
    for page in paginator.paginate():
        clusters.extend(page['clusterArns'])
    return clusters


def get_tasks_by_status(cluster_arn):
    """Get task ARNs grouped by desired status."""
    ecs = boto3.client('ecs')
    tasks_by_status = {}
    
    for status in ['RUNNING', 'PENDING', 'STOPPED']:
        tasks = []
        paginator = ecs.get_paginator('list_tasks')
        try:
            for page in paginator.paginate(cluster=cluster_arn, desiredStatus=status):
                tasks.extend(page.get('taskArns', []))
        except Exception:
            pass
        tasks_by_status[status] = tasks
    
    return tasks_by_status


def get_task_details(cluster_arn, task_arns):
    """Get detailed info for tasks."""
    if not task_arns:
        return []
    ecs = boto3.client('ecs')
    all_tasks = []
    for i in range(0, len(task_arns), 100):
        batch = task_arns[i:i+100]
        response = ecs.describe_tasks(cluster=cluster_arn, tasks=batch)
        all_tasks.extend(response.get('tasks', []))
    return all_tasks


def stop_task(cluster_arn, task_arn, reason):
    """Stop a specific ECS task."""
    ecs = boto3.client('ecs')
    ecs.stop_task(cluster=cluster_arn, task=task_arn, reason=reason)


def print_status_summary(cluster_name, tasks_by_status):
    """Print a summary of tasks by status."""
    print(f"\n  Task Status Summary:")
    print(f"  {'-'*40}")
    
    total = 0
    for status in ['RUNNING', 'PENDING', 'STOPPED']:
        count = len(tasks_by_status.get(status, []))
        total += count
        if count > 0:
            print(f"    {status:12}: {count}")
    
    print(f"  {'-'*40}")
    print(f"    {'TOTAL':12}: {total}")


def main():
    parser = argparse.ArgumentParser(description='Stop ECS tasks running longer than threshold')
    parser.add_argument('--threshold', type=int, default=60, 
                        help='Minutes threshold (default: 60)')
    parser.add_argument('--stop', action='store_true',
                        help='Actually stop the tasks (default is dry-run mode)')
    parser.add_argument('--cluster', type=str, default=None,
                        help='Specific cluster name/ARN (default: all clusters)')
    parser.add_argument('--summary-only', action='store_true',
                        help='Only show status summary, do not stop any tasks')
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    
    if args.cluster:
        clusters = [args.cluster]
    else:
        clusters = get_ecs_clusters()
        print(f"Found {len(clusters)} ECS cluster(s)")

    total_stopped = 0
    
    for cluster_arn in clusters:
        cluster_name = cluster_arn.split('/')[-1] if '/' in cluster_arn else cluster_arn
        print(f"\n{'='*60}")
        print(f"Cluster: {cluster_name}")
        print('='*60)
        
        tasks_by_status = get_tasks_by_status(cluster_arn)
        print_status_summary(cluster_name, tasks_by_status)
        
        if args.summary_only:
            continue
        
        task_arns = tasks_by_status.get('RUNNING', [])
        if not task_arns:
            print("\n  No running tasks to process")
            continue
            
        print(f"\n  Processing {len(task_arns)} running task(s)...")
        
        tasks = get_task_details(cluster_arn, task_arns)
        
        for task in tasks:
            task_id = task['taskArn'].split('/')[-1]
            started_at = task.get('startedAt')
            
            if not started_at:
                print(f"    Task {task_id[:12]}: No start time (pending?), skipping")
                continue
            
            runtime_minutes = (now - started_at).total_seconds() / 60
            task_def = task.get('taskDefinitionArn', 'unknown').split('/')[-1]
            
            if runtime_minutes > args.threshold:
                status = "WOULD STOP" if not args.stop else "STOPPING"
                print(f"    Task {task_id[:12]}: {runtime_minutes:.0f}m ({task_def}) - {status}")
                
                if args.stop:
                    try:
                        stop_task(cluster_arn, task['taskArn'], 
                                  f"Auto-stopped: running {runtime_minutes:.0f}m > {args.threshold}m threshold")
                        total_stopped += 1
                    except Exception as e:
                        print(f"      ERROR stopping task: {e}")
            else:
                print(f"    Task {task_id[:12]}: {runtime_minutes:.0f}m ({task_def}) - OK")

    print(f"\n{'='*60}")
    if args.summary_only:
        print("Summary complete.")
    elif not args.stop:
        print(f"DRY RUN complete. No tasks were stopped.")
    else:
        print(f"Stopped {total_stopped} task(s)")


if __name__ == '__main__':
    main()
