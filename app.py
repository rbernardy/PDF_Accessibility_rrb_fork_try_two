import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_ecr_assets as ecr_assets,
    aws_cloudwatch as cloudwatch,
    aws_events as events,
    aws_events_targets as targets,
    aws_dynamodb as dynamodb,
    aws_ssm as ssm,
)
from constructs import Construct
import platform
import datetime

class PDFAccessibility(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # S3 Bucket
        pdf_processing_bucket = s3.Bucket(self, "pdfaccessibilitybucket1", 
                          encryption=s3.BucketEncryption.S3_MANAGED, 
                          enforce_ssl=True,
                          versioned=True,
                          removal_policy=cdk.RemovalPolicy.RETAIN,
                          cors=[s3.CorsRule(
                              allowed_headers=["*"],
                              allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.HEAD, s3.HttpMethods.PUT, s3.HttpMethods.POST, s3.HttpMethods.DELETE],
                              allowed_origins=["*"],
                              exposed_headers=[]
                          )])
        
        # Get account and region for use throughout the stack
        account_id = Stack.of(self).account
        region = Stack.of(self).region

        # ============================================================
        # Adobe API Rate Limiting Infrastructure (In-Flight Tracking)
        # ============================================================
        
        # SSM Parameter for max in-flight Adobe API requests (configurable without redeployment)
        # This controls how many API calls can be "in progress" at any time across all ECS tasks
        adobe_api_max_in_flight_param_name = '/pdf-processing/adobe-api-max-in-flight'
        adobe_api_max_in_flight_param = ssm.StringParameter(
            self, "AdobeApiMaxInFlightParam",
            parameter_name=adobe_api_max_in_flight_param_name,
            string_value="150",  # Default: 150 concurrent requests (safely under 200 RPM)
            description="Adobe PDF Services API max concurrent in-flight requests"
        )
        
        # SSM Parameter for max requests per minute (RPM limit)
        # Set to 190 to stay safely under Adobe's 200 RPM hard limit with a 10 request buffer
        adobe_api_rpm_param_name = '/pdf-processing/adobe-api-rpm'
        adobe_api_rpm_param = ssm.StringParameter(
            self, "AdobeApiRpmParam",
            parameter_name=adobe_api_rpm_param_name,
            string_value="190",  # 190 RPM limit (10 under Adobe's 200 hard limit for safety buffer)
            description="Adobe PDF Services API max requests per minute (RPM limit)"
        )
        
        # DynamoDB table for distributed in-flight tracking across ECS tasks
        # Uses a single counter that tracks requests currently in progress
        # - Incremented when API call starts
        # - Decremented when API call completes (success or failure)
        # Also stores individual file entries for the in-flight files widget
        adobe_rate_limit_table = dynamodb.Table(
            self, "AdobeInFlightTable",
            table_name="adobe-api-in-flight-tracker",
            partition_key=dynamodb.Attribute(
                name="counter_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"  # Auto-expire file tracking entries after 1 hour
        )

        # Docker images with zstd compression for faster Fargate cold starts
        # zstd decompresses ~2-3x faster than gzip, reducing container startup time
        adobe_autotag_image_asset = ecr_assets.DockerImageAsset(self, "AdobeAutotagImage",
                                                         directory="adobe-autotag-container",
                                                         platform=ecr_assets.Platform.LINUX_AMD64,
                                                         # Enable zstd compression for faster decompression on Fargate
                                                         cache_to=ecr_assets.DockerCacheOption(
                                                             type="inline"
                                                         ),
                                                         outputs=["type=image,compression=zstd,compression-level=3,force-compression=true"])

        alt_text_generator_image_asset = ecr_assets.DockerImageAsset(self, "AltTextGeneratorImage",
                                                             directory="alt-text-generator-container",
                                                             platform=ecr_assets.Platform.LINUX_AMD64,
                                                             # Enable zstd compression for faster decompression on Fargate
                                                             cache_to=ecr_assets.DockerCacheOption(
                                                                 type="inline"
                                                             ),
                                                             outputs=["type=image,compression=zstd,compression-level=3,force-compression=true"])

        # VPC with Public and Private Subnets
        pdf_processing_vpc = ec2.Vpc(self, "PdfProcessingVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PUBLIC,
                    name="PdfProcessingPublic",
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    name="PdfProcessingPrivate",
                    cidr_mask=24,
                ),
            ]
        )

        # VPC Endpoints for faster ECR image pulls (reduces cold start by 10-15s)
        pdf_processing_vpc.add_interface_endpoint("EcrApiEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.ECR
        )
        pdf_processing_vpc.add_interface_endpoint("EcrDockerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER
        )
        pdf_processing_vpc.add_gateway_endpoint("S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3
        )

        # ECS Cluster
        pdf_remediation_cluster = ecs.Cluster(self, "PdfRemediationCluster", vpc=pdf_processing_vpc)

        # Execution role: Used by ECS agent to pull images, write logs, etc.
        ecs_task_execution_role = iam.Role(self, "EcsTaskExecutionRole",
                                 assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
                                 managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ])

        # Task role: Used by the container application for AWS API calls (S3, DynamoDB, SSM, etc.)
        ecs_task_role = iam.Role(self, "EcsTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy"),
            ]
        )
        
        # Bedrock permissions for alt-text generation models
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"],
        ))
        
        # S3 permissions - scoped to the processing bucket only
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            resources=[
                pdf_processing_bucket.bucket_arn,
                f"{pdf_processing_bucket.bucket_arn}/*",
            ],
        ))
        
        # Comprehend permissions for language detection (no resource-level permissions supported)
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=["comprehend:DetectDominantLanguage"],
            resources=["*"],  # Comprehend DetectDominantLanguage does not support resource-level permissions
        ))
        
        # Secrets Manager permissions - scoped to Adobe API credentials
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[f"arn:aws:secretsmanager:{region}:{account_id}:secret:/myapp/*"],
        ))
        
        # CloudWatch Logs permissions for custom metrics logging
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogStreams"
            ],
            resources=[
                f"arn:aws:logs:{region}:{account_id}:log-group:/custom/pdf-remediation/metrics:*"
            ],
        ))
        
        # DynamoDB permissions for Adobe API rate limiting
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:Scan",
            ],
            resources=[adobe_rate_limit_table.table_arn],
        ))
        
        # SSM permissions for reading Adobe API configuration (RPM and max in-flight)
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=["ssm:GetParameter"],
            resources=[
                f"arn:aws:ssm:{region}:{account_id}:parameter{adobe_api_rpm_param_name}",
                f"arn:aws:ssm:{region}:{account_id}:parameter{adobe_api_max_in_flight_param_name}",
            ],
        ))
        
        # Lambda invoke permission for PDF failure analysis
        ecs_task_role.add_to_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[f"arn:aws:lambda:{region}:{account_id}:function:pdf-failure-analysis"],
        ))
        
        # Grant S3 read/write access to ECS Task Role
        pdf_processing_bucket.grant_read_write(ecs_task_execution_role)
        # Create ECS Task Log Groups explicitly
        adobe_autotag_log_group = logs.LogGroup(self, "AdobeAutotagContainerLogs",
                                                log_group_name="/ecs/pdf-remediation/adobe-autotag",
                                                retention=logs.RetentionDays.ONE_MONTH,
                                                removal_policy=cdk.RemovalPolicy.DESTROY)

        alt_text_generator_log_group = logs.LogGroup(self, "AltTextGeneratorContainerLogs",
                                                    log_group_name="/ecs/pdf-remediation/alt-text-generator",
                                                    retention=logs.RetentionDays.ONE_MONTH,
                                                    removal_policy=cdk.RemovalPolicy.DESTROY)

        # Custom logging for PDF processing metrics (Adobe API calls, errors, failures)
        pdf_processing_metrics_log_group = logs.LogGroup(self, "PdfProcessingMetricsLogs",
                                                         log_group_name="/custom/pdf-remediation/metrics",
                                                         retention=logs.RetentionDays.ONE_MONTH,
                                                         removal_policy=cdk.RemovalPolicy.DESTROY)
        # ECS Task Definitions - Updated for large PDF support
        adobe_autotag_task_def = ecs.FargateTaskDefinition(self, "AdobeAutotagTaskDefinition",
                                                      memory_limit_mib=4096,  # Increased from 1024 for large PDFs
                                                      cpu=1024,  # Increased from 256 for large PDFs
                                                      execution_role=ecs_task_execution_role, 
                                                      task_role=ecs_task_role,
                                                      family="PDFAccessibilityAdobeAutotagTaskDefinitionV2"  # Force new version
                                                     )

        adobe_autotag_container_def = adobe_autotag_task_def.add_container("adobe-autotag-container",
                                                                  image=ecs.ContainerImage.from_registry(adobe_autotag_image_asset.image_uri),
                                                                  memory_limit_mib=4096,  # Increased from 1024 for large PDFs
                                                                  logging=ecs.LogDrivers.aws_logs(
        stream_prefix="AdobeAutotagLogs",
        log_group=adobe_autotag_log_group,
    ))

        alt_text_task_def = ecs.FargateTaskDefinition(self, "AltTextGenerationTaskDefinition",
                                                      memory_limit_mib=4096,  # Increased from 1024 for large PDFs
                                                      cpu=1024,  # Increased from 256 for large PDFs
                                                      execution_role=ecs_task_execution_role, 
                                                      task_role=ecs_task_role,
                                                      family="PDFAccessibilityAltTextTaskDefinitionV2"  # Force new version
                                                      )

        alt_text_container_def = alt_text_task_def.add_container("alt-text-llm-container",
                                                                  image=ecs.ContainerImage.from_registry(alt_text_generator_image_asset.image_uri),
                                                                  memory_limit_mib=4096,  # Increased from 1024 for large PDFs
                                                                   logging=ecs.LogDrivers.aws_logs(
        stream_prefix="AltTextGeneratorLogs",
        log_group=alt_text_generator_log_group
    ))
        # ECS Tasks in Step Functions
        adobe_autotag_task = tasks.EcsRunTask(self, "RunAdobeAutotagTask",
                                      integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                      cluster=pdf_remediation_cluster,
                                      task_definition=adobe_autotag_task_def,
                                      assign_public_ip=False,
                                      result_path="$.ecs_task_1_result",
                                      container_overrides=[tasks.ContainerOverride(
                                       container_definition = adobe_autotag_container_def,
                                          environment=[
                                              tasks.TaskEnvironmentVariable(
                                                  name="S3_BUCKET_NAME",
                                                  value=sfn.JsonPath.string_at("$.s3_bucket")
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="S3_FILE_KEY",
                                                  value=sfn.JsonPath.string_at("$.s3_key")
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="S3_CHUNK_KEY",
                                                  value=sfn.JsonPath.string_at("$.chunk_key")
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="AWS_REGION",
                                                  value=region
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="RATE_LIMIT_TABLE",
                                                  value=adobe_rate_limit_table.table_name
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="ADOBE_API_MAX_IN_FLIGHT_PARAM",
                                                  value=adobe_api_max_in_flight_param_name
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="ADOBE_API_RPM_PARAM",
                                                  value=adobe_api_rpm_param_name
                                              ),
                                          ]
                                      )],
                                      launch_target=tasks.EcsFargateLaunchTarget(
                                          platform_version=ecs.FargatePlatformVersion.LATEST
                                      ),
                                      propagated_tag_source=ecs.PropagatedTagSource.TASK_DEFINITION,
                                     )

        alt_text_generation_task = tasks.EcsRunTask(self, "RunAltTextGenerationTask",
                                      integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                                      cluster=pdf_remediation_cluster,
                                      task_definition=alt_text_task_def,
                                      assign_public_ip=False,
                                    
                                      container_overrides=[tasks.ContainerOverride(
                                          container_definition=alt_text_container_def,
                                          environment=[
                                              tasks.TaskEnvironmentVariable(
                                                  name="S3_BUCKET_NAME",
                                                  value=sfn.JsonPath.string_at("$.s3_bucket")
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="S3_FILE_KEY",
                                                  value=sfn.JsonPath.string_at("$.s3_key")
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="S3_CHUNK_KEY",
                                                  value=sfn.JsonPath.string_at("$.chunk_key")
                                              ),
                                              tasks.TaskEnvironmentVariable(
                                                  name="AWS_REGION",
                                                  value=region
                                              ),
                                          ]
                                      )],
                                      launch_target=tasks.EcsFargateLaunchTarget(
                                          platform_version=ecs.FargatePlatformVersion.LATEST
                                      ),
                                      propagated_tag_source=ecs.PropagatedTagSource.TASK_DEFINITION,
                                      )

        # Step Function Map State
        # max_concurrency controls parallel ECS tasks - can be set high for fast processing
        # Rate limiting is handled by in-flight tracking in the ECS container:
        # - Each task acquires a "slot" before making Adobe API calls
        # - Slots are released when API calls complete (success or failure)
        # - Tasks wait if max_in_flight limit is reached
        # This allows high concurrency for non-API work while preventing API rate limit errors
        pdf_chunks_map_state = sfn.Map(self, "ProcessPdfChunksInParallel",
                            max_concurrency=100,
                            items_path=sfn.JsonPath.string_at("$.chunks"),
                            result_path="$.MapResults")

        pdf_chunks_map_state.iterator(adobe_autotag_task.next(alt_text_generation_task))

        cloudwatch_metrics_policy = iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],  # Allow PutMetricData action
                    resources=["*"],  # All CloudWatch resources # All CloudWatch Logs resources
        )
        pdf_merger_lambda = lambda_.Function(
            self, 'PdfMergerLambda',
            runtime=lambda_.Runtime.JAVA_21,
            handler='com.example.App::handleRequest',
            code=lambda_.Code.from_asset('lambda/pdf-merger-lambda/PDFMergerLambda/target/PDFMergerLambda-1.0-SNAPSHOT.jar'),
            environment={
                'BUCKET_NAME': pdf_processing_bucket.bucket_name  # this line sets the environment variable
            },
            timeout=Duration.seconds(900),
            memory_size=3008,  # Increased from 1024 for large PDFs
            ephemeral_storage_size=cdk.Size.mebibytes(2048)  # 2GB /tmp storage
        )

        pdf_merger_lambda.add_to_role_policy(cloudwatch_metrics_policy)
        pdf_merger_lambda_task = tasks.LambdaInvoke(self, "MergePdfChunks",
                                      lambda_function=pdf_merger_lambda,
                                      payload=sfn.TaskInput.from_object({
        "fileNames.$": "$.chunks[*].chunk_key"
                     }),
                                      result_selector={
                                          "java_output.$": "$.Payload"
                                      })
        pdf_processing_bucket.grant_read_write(pdf_merger_lambda)

        # Define the Add Title Lambda function
        host_machine = platform.machine().lower()
        print("Architecture of Machine:",host_machine)
        if "arm" in host_machine:
            lambda_arch = lambda_.Architecture.ARM_64
        else:
            lambda_arch = lambda_.Architecture.X86_64

        title_generator_lambda = lambda_.Function(
            self, 'BedrockTitleGeneratorLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='title_generator.lambda_handler',
            code=lambda_.Code.from_docker_build('lambda/title-generator-lambda'),
            timeout=Duration.seconds(900),
            memory_size=3008,  # Increased from 1024 for large PDFs
            ephemeral_storage_size=cdk.Size.mebibytes(2048),  # 2GB /tmp storage
            architecture=lambda_arch,
        )

        # Grant the Lambda function read/write permissions to the S3 bucket
        pdf_processing_bucket.grant_read_write(title_generator_lambda)

        # Define the task to invoke the Add Title Lambda function
        title_generator_lambda_task = tasks.LambdaInvoke(
            self, "GenerateAccessibleTitle",
            lambda_function=title_generator_lambda,
            payload=sfn.TaskInput.from_json_path_at("$")
        )

        # Add the necessary policy to the Lambda function's role
        title_generator_lambda.add_to_role_policy(cloudwatch_metrics_policy)
        
        # Bedrock permissions for title generation models
        title_generator_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"],
        ))

        # Chain the tasks in the state machine
        # chain = pdf_chunks_map_state.next(pdf_merger_lambda_task).next(title_generator_lambda_task)
        
        pre_remediation_accessibility_checker = lambda_.Function(
            self,'PreRemediationAccessibilityAuditor',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='main.lambda_handler',
            code=lambda_.Code.from_docker_build('lambda/pre-remediation-accessibility-checker'),
            timeout=Duration.seconds(900),
            memory_size=2048,  # Increased from 512 for large PDFs
            ephemeral_storage_size=cdk.Size.mebibytes(2048),  # 2GB /tmp storage
            architecture=lambda_arch,
        )
        
        pre_remediation_accessibility_checker.add_to_role_policy(
            iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[f"arn:aws:secretsmanager:{region}:{account_id}:secret:/myapp/*"]
        ))
        pdf_processing_bucket.grant_read_write(pre_remediation_accessibility_checker)
        pre_remediation_accessibility_checker.add_to_role_policy(cloudwatch_metrics_policy)

        pre_remediation_accessibility_checker_task = tasks.LambdaInvoke(
            self, 
            "AuditPreRemediationAccessibility",
            lambda_function=pre_remediation_accessibility_checker,
            payload=sfn.TaskInput.from_json_path_at("$"),
            output_path="$.Payload"
        )

        post_remediation_accessibility_checker = lambda_.Function(
            self,'PostRemediationAccessibilityAuditor',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='main.lambda_handler',
            code=lambda_.Code.from_docker_build('lambda/post-remediation-accessibility-checker'),
            timeout=Duration.seconds(900),
            memory_size=2048,  # Increased from 512 for large PDFs
            ephemeral_storage_size=cdk.Size.mebibytes(2048),  # 2GB /tmp storage
            architecture=lambda_arch,
        )
        
        post_remediation_accessibility_checker.add_to_role_policy(
            iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[f"arn:aws:secretsmanager:{region}:{account_id}:secret:/myapp/*"]
        ))
        pdf_processing_bucket.grant_read_write(post_remediation_accessibility_checker)
        post_remediation_accessibility_checker.add_to_role_policy(cloudwatch_metrics_policy)

        post_remediation_accessibility_checker_task = tasks.LambdaInvoke(
            self, 
            "AuditPostRemediationAccessibility",
            lambda_function=post_remediation_accessibility_checker,
            payload=sfn.TaskInput.from_json_path_at("$"),
            output_path="$.Payload"
        )

        # PDF Report Generator Lambda - generates Excel reports of processed PDFs
        pdf_report_generator_lambda = lambda_.Function(
            self, 'PdfReportGeneratorLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='main.lambda_handler',
            code=lambda_.Code.from_docker_build(
                'lambda/pdf-report-generator',
                build_args={
                    'BUILD_DATE': datetime.datetime.now().isoformat()
                }
            ),
            timeout=Duration.seconds(900),
            memory_size=1024,
            ephemeral_storage_size=cdk.Size.mebibytes(512),
            architecture=lambda_arch,
            environment={
                'BUCKET_NAME': pdf_processing_bucket.bucket_name,
                'TZ': 'US/Eastern'  # Set timezone for local time in filenames
            }
        )
        
        # Grant S3 read/write access for reading PDFs/JSONs and writing CSV reports
        pdf_processing_bucket.grant_read_write(pdf_report_generator_lambda)
        pdf_report_generator_lambda.add_to_role_policy(cloudwatch_metrics_policy)
        
        # Schedule the report generator to run daily at midnight UTC
        pdf_report_schedule = events.Rule(
            self, 'PdfReportSchedule',
            schedule=events.Schedule.cron(minute='0', hour='0'),
            description='Daily schedule for PDF processing report generation'
        )
        pdf_report_schedule.add_target(targets.LambdaFunction(pdf_report_generator_lambda))
        
        remediation_chain = pdf_chunks_map_state.next(pdf_merger_lambda_task).next(title_generator_lambda_task).next(post_remediation_accessibility_checker_task)

        parallel_accessibility_workflow = sfn.Parallel(self, "ParallelAccessibilityWorkflow",
                                      result_path="$.ParallelResults")
        parallel_accessibility_workflow.branch(remediation_chain)
        parallel_accessibility_workflow.branch(pre_remediation_accessibility_checker_task)

        pdf_remediation_workflow_log_group = logs.LogGroup(self, "PdfRemediationWorkflowLogs",
            log_group_name="/aws/states/pdf-accessibility-remediation-workflow",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        # State Machine

        pdf_remediation_state_machine = sfn.StateMachine(self, "PdfAccessibilityRemediationWorkflow",
                                         definition=parallel_accessibility_workflow,
                                         timeout=Duration.minutes(180),     # update to 180 from 150
                                         logs=sfn.LogOptions(
                                             destination=pdf_remediation_workflow_log_group,
                                             level=sfn.LogLevel.ALL
                                         ))

        # Pipeline Status Checker Lambda - monitors if pipeline is processing or idle
        pipeline_status_checker_lambda = lambda_.Function(
            self, 'PipelineStatusCheckerLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='main.lambda_handler',
            code=lambda_.Code.from_docker_build(
                'lambda/pipeline-status-checker',
                build_args={
                    'BUILD_DATE': datetime.datetime.now().isoformat()
                }
            ),
            timeout=Duration.seconds(60),
            memory_size=256,
            architecture=lambda_arch,
            environment={
                'STATE_MACHINE_ARN': pdf_remediation_state_machine.state_machine_arn,
                'ECS_CLUSTER_NAME': pdf_remediation_cluster.cluster_name
            }
        )
        
        # Grant permissions to check Step Function executions
        pdf_remediation_state_machine.grant_read(pipeline_status_checker_lambda)
        
        # Grant permissions to list ECS tasks
        pipeline_status_checker_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=['ecs:ListTasks'],
            resources=['*']
        ))
        
        # Grant permissions to publish CloudWatch metrics
        pipeline_status_checker_lambda.add_to_role_policy(cloudwatch_metrics_policy)
        
        # Schedule the status checker to run every minute
        pipeline_status_schedule = events.Rule(
            self, 'PipelineStatusSchedule',
            schedule=events.Schedule.rate(Duration.minutes(1)),
            description='Every minute check of PDF processing pipeline status'
        )
        pipeline_status_schedule.add_target(targets.LambdaFunction(pipeline_status_checker_lambda))
        
        # Store the log group name for the dashboard
        pipeline_status_log_group_name = f"/aws/lambda/{pipeline_status_checker_lambda.function_name}"
        
        # Lambda Function
        pdf_splitter_lambda = lambda_.Function(
            self, 'PdfChunkSplitterLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='main.lambda_handler',
            code=lambda_.Code.from_docker_build("lambda/pdf-splitter-lambda"),
            timeout=Duration.seconds(900),
            memory_size=3008,  # Increased from 1024 for large PDFs
            ephemeral_storage_size=cdk.Size.mebibytes(2048)  # 2GB /tmp storage
        )

        pdf_splitter_lambda.add_to_role_policy(cloudwatch_metrics_policy)

        # S3 Permissions for Lambda
        pdf_processing_bucket.grant_read_write(pdf_splitter_lambda)

        # Trigger Lambda on S3 Event
        pdf_processing_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(pdf_splitter_lambda),
            s3.NotificationKeyFilter(prefix="pdf/"),
            s3.NotificationKeyFilter(suffix=".pdf")
        )

        # Step Function Execution Permissions
        pdf_remediation_state_machine.grant_start_execution(pdf_splitter_lambda)

        # Pass State Machine ARN to Lambda as an Environment Variable
        pdf_splitter_lambda.add_environment("STATE_MACHINE_ARN", pdf_remediation_state_machine.state_machine_arn)
        # Store log group names dynamically
        pdf_splitter_lambda_log_group_name = f"/aws/lambda/{pdf_splitter_lambda.function_name}"
        pdf_merger_lambda_log_group_name = f"/aws/lambda/{pdf_merger_lambda.function_name}"
        title_generator_lambda_log_group_name = f"/aws/lambda/{title_generator_lambda.function_name}"
        pre_remediation_checker_log_group_name = f"/aws/lambda/{pre_remediation_accessibility_checker.function_name}"
        post_remediation_checker_log_group_name = f"aws/lambda/{post_remediation_accessibility_checker.function_name}"



        # S3 PDF Copier Lambda - copies remediated PDFs to destination bucket
        # Get destination bucket from context (pass via -c flag or cdk.context.json)
        destination_bucket_name = self.node.try_get_context("destination_bucket")
        
        if destination_bucket_name:
            destination_bucket = s3.Bucket.from_bucket_name(
                self, "DestinationBucket", destination_bucket_name
            )
            
            s3_pdf_copier_lambda = lambda_.Function(
                self, 'S3PdfCopierLambda',
                runtime=lambda_.Runtime.PYTHON_3_12,
                architecture=lambda_.Architecture.ARM_64,
                handler='main.handler',
                code=lambda_.Code.from_asset('lambda/s3-pdf-copier'),
                memory_size=128,
                timeout=Duration.seconds(30),
                environment={
                    'DESTINATION_BUCKET': destination_bucket_name,
                },
            )
            
            # Create log group explicitly for the dashboard
            s3_pdf_copier_log_group = logs.LogGroup(
                self, 'S3PdfCopierLogGroup',
                log_group_name=f"/aws/lambda/{s3_pdf_copier_lambda.function_name}",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=cdk.RemovalPolicy.DESTROY
            )
            
            # Store the log group name for the dashboard
            s3_pdf_copier_log_group_name = s3_pdf_copier_log_group.log_group_name
            
            # Grant permissions
            pdf_processing_bucket.grant_read(s3_pdf_copier_lambda)
            destination_bucket.grant_write(s3_pdf_copier_lambda)
            
            # Add S3 event notification for result/ prefix
            pdf_processing_bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.LambdaDestination(s3_pdf_copier_lambda),
                s3.NotificationKeyFilter(prefix="result/"),
            )

        # =============================================================================
        # PDF Failure Cleanup Feature - Auto-delete PDF and temp files on pipeline failure
        # =============================================================================
        
        # DynamoDB table for notification preferences (IAM username -> email)
        pdf_cleanup_notification_table = dynamodb.Table(
            self, "PdfCleanupNotificationTable",
            table_name="pdf-cleanup-notifications",
            partition_key=dynamodb.Attribute(
                name="iam_username",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            point_in_time_recovery=True
        )
        
        # DynamoDB table for failure records (for daily digest)
        pdf_failure_records_table = dynamodb.Table(
            self, "PdfFailureRecordsTable",
            table_name="pdf-failure-records",
            partition_key=dynamodb.Attribute(
                name="failure_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            time_to_live_attribute="ttl"  # Auto-delete old records after 30 days
        )
        
        # Add GSI for querying by date
        pdf_failure_records_table.add_global_secondary_index(
            index_name="failure_date-index",
            partition_key=dynamodb.Attribute(
                name="failure_date",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # CloudWatch log group for cleanup events
        pdf_cleanup_log_group = logs.LogGroup(
            self, "PdfCleanupLogGroup",
            log_group_name="/pdf-processing/cleanup",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=cdk.RemovalPolicy.RETAIN
        )
        
        # Lambda function for PDF failure cleanup (triggered by Step Function failures)
        pdf_failure_cleanup_lambda = lambda_.Function(
            self, "PdfFailureCleanupLambda",
            function_name="pdf-failure-cleanup-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=lambda_.Code.from_docker_build(
                "lambda/pdf-failure-cleanup",
                build_args={
                    "BUILD_DATE": datetime.datetime.now().isoformat()
                }
            ),
            memory_size=256,
            timeout=Duration.minutes(5),
            architecture=lambda_arch,
            environment={
                "FAILURE_TABLE": pdf_failure_records_table.table_name,
                "LOG_GROUP_NAME": pdf_cleanup_log_group.log_group_name,
                "BUCKET_NAME": pdf_processing_bucket.bucket_name
            }
        )
        
        # Grant permissions to cleanup Lambda
        pdf_processing_bucket.grant_read(pdf_failure_cleanup_lambda)
        pdf_processing_bucket.grant_delete(pdf_failure_cleanup_lambda)
        pdf_failure_records_table.grant_write_data(pdf_failure_cleanup_lambda)
        pdf_cleanup_log_group.grant_write(pdf_failure_cleanup_lambda)
        
        # CloudTrail permissions for identifying who uploaded the file
        pdf_failure_cleanup_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["cloudtrail:LookupEvents"],
                resources=["*"]
            )
        )
        
        # CloudWatch Logs read permissions for looking up actual ECS task errors
        pdf_failure_cleanup_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:FilterLogEvents",
                    "logs:GetLogEvents"
                ],
                resources=[
                    f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/pdf-remediation/adobe-autotag:*",
                    f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/pdf-remediation/alt-text-generator:*"
                ]
            )
        )
        
        # EventBridge rule to trigger cleanup on Step Function failures
        pdf_failure_rule = events.Rule(
            self, "PdfProcessingFailureRule",
            rule_name="pdf-processing-failure-cleanup",
            description="Triggers cleanup when PDF processing Step Function fails",
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "stateMachineArn": [pdf_remediation_state_machine.state_machine_arn],
                    "status": ["FAILED", "TIMED_OUT", "ABORTED"]
                }
            )
        )
        pdf_failure_rule.add_target(targets.LambdaFunction(pdf_failure_cleanup_lambda))
        
        # SSM Parameters for digest configuration (can be changed without redeploying)
        # Email enabled: aws ssm put-parameter --name "/pdf-processing/email-enabled" --value "true" --type String
        # Sender email: aws ssm put-parameter --name "/pdf-processing/sender-email" --value "your-email@domain.com" --type String
        sender_email_param_name = "/pdf-processing/sender-email"
        email_enabled_param_name = "/pdf-processing/email-enabled"
        
        # Lambda function for daily digest (triggered at 11:55 PM)
        # If email disabled, saves reports to S3: reports/deletion_reports/{username}/{username}-{timestamp}.txt
        pdf_failure_digest_lambda = lambda_.Function(
            self, "PdfFailureDigestLambda",
            function_name="pdf-failure-digest-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=lambda_.Code.from_docker_build(
                "lambda/pdf-failure-digest",
                build_args={
                    "BUILD_DATE": datetime.datetime.now().isoformat()
                }
            ),
            memory_size=256,
            timeout=Duration.minutes(5),
            architecture=lambda_arch,
            environment={
                "FAILURE_TABLE": pdf_failure_records_table.table_name,
                "NOTIFICATION_TABLE": pdf_cleanup_notification_table.table_name,
                "SENDER_EMAIL_PARAM": sender_email_param_name,
                "EMAIL_ENABLED_PARAM": email_enabled_param_name,
                "BUCKET_NAME": pdf_processing_bucket.bucket_name
            }
        )
        
        # Grant permissions to digest Lambda
        pdf_failure_records_table.grant_read_write_data(pdf_failure_digest_lambda)
        pdf_cleanup_notification_table.grant_read_data(pdf_failure_digest_lambda)
        
        # S3 permissions for saving reports
        pdf_processing_bucket.grant_write(pdf_failure_digest_lambda, "reports/deletion_reports/*")
        
        # SSM permissions to read configuration parameters
        pdf_failure_digest_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[
                    f"arn:aws:ssm:{region}:{account_id}:parameter{sender_email_param_name}",
                    f"arn:aws:ssm:{region}:{account_id}:parameter{email_enabled_param_name}"
                ]
            )
        )
        
        # SES permissions for sending notification emails (when enabled)
        pdf_failure_digest_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ses:SendEmail"],
                resources=["*"]
            )
        )
        
        # CloudWatch Logs permissions to read ECS container logs for detailed error lookup
        pdf_failure_digest_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:FilterLogEvents",
                    "logs:GetLogEvents"
                ],
                resources=[
                    f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/pdf-remediation/adobe-autotag:*",
                    f"arn:aws:logs:{region}:{account_id}:log-group:/ecs/pdf-remediation/alt-text-generator:*"
                ]
            )
        )
        
        # Schedule daily digest at 11:55 PM UTC
        pdf_digest_schedule = events.Rule(
            self, "PdfFailureDigestSchedule",
            rule_name="pdf-failure-digest-daily",
            description="Daily digest of PDF processing failures at 11:55 PM",
            schedule=events.Schedule.cron(minute="55", hour="23")
        )
        pdf_digest_schedule.add_target(targets.LambdaFunction(pdf_failure_digest_lambda))
        
        # Store log group name for dashboard
        pdf_cleanup_log_group_name = pdf_cleanup_log_group.log_group_name

        # =============================================================================
        # PDF Failure Analysis Lambda - Analyzes PDFs that fail Adobe API processing
        # =============================================================================
        
        # Log group for failure analysis
        pdf_failure_analysis_log_group = logs.LogGroup(
            self, "PdfFailureAnalysisLogGroup",
            log_group_name="/lambda/pdf-failure-analysis",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        
        pdf_failure_analysis_lambda = lambda_.DockerImageFunction(
            self, "PdfFailureAnalysisLambda",
            function_name="pdf-failure-analysis",
            code=lambda_.DockerImageCode.from_image_asset("lambda/pdf-failure-analysis"),
            memory_size=1024,
            timeout=Duration.seconds(60),
            ephemeral_storage_size=cdk.Size.mebibytes(1024),
            architecture=lambda_arch,
            environment={
                "REPORT_BUCKET": pdf_processing_bucket.bucket_name,
                "SAVE_REPORTS_TO_S3": "true"
            },
            log_group=pdf_failure_analysis_log_group
        )
        
        # Grant S3 read access for downloading PDFs to analyze
        pdf_processing_bucket.grant_read(pdf_failure_analysis_lambda)
        # Grant S3 write access for saving reports
        pdf_processing_bucket.grant_write(pdf_failure_analysis_lambda, "reports/failure_analysis/*")
        
        # Store log group name for dashboard
        pdf_failure_analysis_log_group_name = pdf_failure_analysis_log_group.log_group_name

        # =============================================================================
        # Rate Limit Widget Lambda - Custom CloudWatch widget for real-time in-flight status
        # =============================================================================
        
        rate_limit_widget_lambda = lambda_.Function(
            self, "RateLimitWidgetLambda",
            function_name="rate-limit-widget",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset("lambda/rate-limit-widget"),
            memory_size=128,
            timeout=Duration.seconds(10),
            architecture=lambda_arch,
            environment={
                "RATE_LIMIT_TABLE": adobe_rate_limit_table.table_name,
                "ADOBE_API_MAX_IN_FLIGHT_PARAM": adobe_api_max_in_flight_param_name,
                "ADOBE_API_RPM_PARAM": adobe_api_rpm_param_name
            }
        )
        
        # Grant permissions to read from DynamoDB and SSM
        adobe_rate_limit_table.grant_read_data(rate_limit_widget_lambda)
        rate_limit_widget_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[
                    f"arn:aws:ssm:{region}:{account_id}:parameter{adobe_api_max_in_flight_param_name}",
                    f"arn:aws:ssm:{region}:{account_id}:parameter{adobe_api_rpm_param_name}"
                ]
            )
        )
        
        # Grant CloudWatch permission to invoke the custom widget Lambda
        rate_limit_widget_lambda.grant_invoke(iam.ServicePrincipal("cloudwatch.amazonaws.com"))

        # =============================================================================
        # In-Flight Files Widget Lambda - Shows list of files currently using API slots
        # =============================================================================
        
        in_flight_files_widget_lambda = lambda_.Function(
            self, "InFlightFilesWidgetLambda",
            function_name="in-flight-files-widget",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset("lambda/in-flight-files-widget"),
            memory_size=128,
            timeout=Duration.seconds(10),
            architecture=lambda_arch,
            environment={
                "RATE_LIMIT_TABLE": adobe_rate_limit_table.table_name,
                "ADOBE_API_MAX_IN_FLIGHT_PARAM": adobe_api_max_in_flight_param_name
            }
        )
        
        # Grant permissions to read from DynamoDB and SSM
        adobe_rate_limit_table.grant_read_data(in_flight_files_widget_lambda)
        in_flight_files_widget_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["ssm:GetParameter"],
                resources=[
                    f"arn:aws:ssm:{region}:{account_id}:parameter{adobe_api_max_in_flight_param_name}"
                ]
            )
        )
        
        # Grant CloudWatch permission to invoke the custom widget Lambda
        in_flight_files_widget_lambda.grant_invoke(iam.ServicePrincipal("cloudwatch.amazonaws.com"))

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        dashboard_name = f"PDF_Processing_Dashboard-{timestamp}"
        dashboard = cloudwatch.Dashboard(self, "PdfRemediationMonitoringDashboard", dashboard_name=dashboard_name,
                                         variables=[cloudwatch.DashboardVariable(
                                            id="filename",
                                            type=cloudwatch.VariableType.PATTERN,
                                            label="File Name",
                                            input_type=cloudwatch.VariableInputType.INPUT,
                                            value="filename",
                                            visible=True,
                                            default_value=cloudwatch.DefaultValue.value(".*"),
                                        )]
                                         )
        # Add Widgets to the Dashboard
        dashboard.add_widgets(
            # Row 1: Pipeline Status (full width)
            cloudwatch.LogQueryWidget(
                title="Pipeline Status",
                log_group_names=[pipeline_status_log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @message like /PIPELINE_STATUS/
                    | parse @message 'PIPELINE_STATUS: *' as status_json
                    | parse status_json '"status":"*"' as status
                    | parse status_json '"running_executions":*,' as executions
                    | parse status_json '"running_ecs_tasks":*,' as tasks
                    | display @timestamp, status, executions, tasks
                    | sort @timestamp desc
                    | limit 100''',
                width=24,
                height=6
            ),
            # Row 2: In-Flight API Status (custom widget - full width)
            cloudwatch.CustomWidget(
                title="Adobe API In-Flight Requests (Real-time)",
                function_arn=rate_limit_widget_lambda.function_arn,
                width=12,
                height=4,
                update_on_refresh=True,
                update_on_resize=False,
                update_on_time_range_change=False
            ),
            # Row 2: In-Flight Files List (custom widget - side by side)
            cloudwatch.CustomWidget(
                title="Files Currently In-Flight",
                function_arn=in_flight_files_widget_lambda.function_arn,
                width=12,
                height=4,
                update_on_refresh=True,
                update_on_resize=False,
                update_on_time_range_change=False
            ),
            # Row 3: File Status (full width) - moved up
            cloudwatch.LogQueryWidget(
                title="File Status",
                log_group_names=[pdf_splitter_lambda_log_group_name, pdf_merger_lambda_log_group_name, adobe_autotag_log_group.log_group_name, alt_text_generator_log_group.log_group_name],
                query_string='''fields @timestamp, @message
                    | parse @message "File: *, Status: *" as file, status
                    | stats latest(status) as latestStatus, max(@timestamp) as lastUpdated by file
                    | sort file asc ''',
                width=24,
                height=6
            ),
            # Row 4: In-Flight tracking logs and DynamoDB activity (side by side)
            cloudwatch.LogQueryWidget(
                title="Adobe API In-Flight Tracking (Logs)",
                log_group_names=[adobe_autotag_log_group.log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @message like /In-flight status/ or @message like /Released rate limit slot/ or @message like /Acquired slot/
                    | display @timestamp, @message
                    | sort @timestamp desc
                    | limit 50''',
                width=12,
                height=4
            ),
            cloudwatch.GraphWidget(
                title="Adobe API In-Flight Table Activity",
                left=[
                    adobe_rate_limit_table.metric_consumed_write_capacity_units(
                        statistic="Sum",
                        period=Duration.minutes(1)
                    ),
                ],
                width=12,
                height=4
            ),
            # Row 5: Adobe API Calls (full width)
            cloudwatch.LogQueryWidget(
                title="Adobe API Calls",
                log_group_names=[pdf_processing_metrics_log_group.log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @logStream like /adobe-api-calls/
                    | sort @timestamp desc
                    | limit 100''',
                width=24,
                height=6
            ),
            # Row 6: Adobe API Errors (full width)
            cloudwatch.LogQueryWidget(
                title="Adobe API Errors",
                log_group_names=[pdf_processing_metrics_log_group.log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @logStream like /adobe-api-errors/
                    | sort @timestamp desc
                    | limit 200''',
                width=24,
                height=6
            ),
            # Row 7: PDF Failure Analysis Results (full width)
            cloudwatch.LogQueryWidget(
                title="PDF Failure Analysis",
                log_group_names=[pdf_failure_analysis_log_group_name],
                query_string='''fields @timestamp, @message
| filter @message like /PDF_FAILURE_ANALYSIS/
| sort @timestamp desc
| limit 50''',
                width=24,
                height=6
            ),
            # Row 8: Processing Failures (full width)
            cloudwatch.LogQueryWidget(
                title="Processing Failures",
                log_group_names=[pdf_cleanup_log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @message like /PIPELINE_FAILURE_CLEANUP/
                    | sort @timestamp desc
                    | limit 50''',
                width=24,
                height=6
            ),
            # Row 8: Split PDF Lambda Logs (full width)
            cloudwatch.LogQueryWidget(
                title="Split PDF Lambda Logs",
                log_group_names=[pdf_splitter_lambda_log_group_name],
                query_string='''fields @message 
                                | filter @message like /filename/''',
                width=24,
                height=6
            ),
            cloudwatch.LogQueryWidget(
                title="Step Function Execution Logs",
                log_group_names=[pdf_remediation_workflow_log_group.log_group_name],
                query_string='''fields @message 
                                | filter @message like /filename/''',
                width=24,
                height=6
            ),
            cloudwatch.LogQueryWidget(
                title="Adobe Autotag Processing Logs",
                log_group_names=[adobe_autotag_log_group.log_group_name],
                query_string='''fields @message 
                                | filter @message like /filename/''',
                width=24,
                height=6
            ),
            cloudwatch.LogQueryWidget(
                title="Alt Text Generation Logs",
                log_group_names=[alt_text_generator_log_group.log_group_name],
                query_string='''fields @message 
                                | filter @message like /filename/''',
                width=24,
                height=6
            ),
            cloudwatch.LogQueryWidget(
                title="PDF Merger Lambda Logs",
                log_group_names=[pdf_merger_lambda_log_group_name],
                query_string='''fields @message 
                                | filter @message like /filename/''',
                width=24,
                height=6
            ),
        )
        
        # Add S3 PDF Copier widgets to main dashboard if destination bucket is configured
        if destination_bucket_name:
            dashboard.add_widgets(
                cloudwatch.LogQueryWidget(
                    title="S3 PDF Copier - All Logs",
                    log_group_names=[s3_pdf_copier_log_group_name],
                    query_string='''fields @timestamp, @message
                        | sort @timestamp desc
                        | limit 100''',
                    width=24,
                    height=8
                ),
                cloudwatch.LogQueryWidget(
                    title="S3 PDF Copier - Successful Copies",
                    log_group_names=[s3_pdf_copier_log_group_name],
                    query_string='''fields @timestamp, @message
                        | filter @message like /Successfully copied/
                        | parse @message "Successfully copied to *" as destination
                        | display @timestamp, destination
                        | sort @timestamp desc
                        | limit 50''',
                    width=12,
                    height=6
                ),
                cloudwatch.LogQueryWidget(
                    title="S3 PDF Copier - Errors",
                    log_group_names=[s3_pdf_copier_log_group_name],
                    query_string='''fields @timestamp, @message
                        | filter @message like /ERROR/ or @message like /Failed/
                        | sort @timestamp desc
                        | limit 50''',
                    width=12,
                    height=6
                ),
                cloudwatch.LogQueryWidget(
                    title="S3 PDF Copier - File Processing Details",
                    log_group_names=[s3_pdf_copier_log_group_name],
                    query_string='''fields @timestamp, @message
                        | filter @message like /Source bucket/ or @message like /Destination bucket/
                        | parse @message "Source bucket: *, key: *" as source_bucket, source_key
                        | parse @message "Destination bucket: *, key: *" as dest_bucket, dest_key
                        | display @timestamp, source_bucket, source_key, dest_bucket, dest_key
                        | sort @timestamp desc
                        | limit 50''',
                    width=24,
                    height=6
                ),
                cloudwatch.LogQueryWidget(
                    title="S3 PDF Copier - Skipped Files",
                    log_group_names=[s3_pdf_copier_log_group_name],
                    query_string='''fields @timestamp, @message
                        | filter @message like /Skipping non-PDF/
                        | parse @message "Skipping non-PDF file: *" as skipped_file
                        | display @timestamp, skipped_file
                        | sort @timestamp desc
                        | limit 50''',
                    width=24,
                    height=6
                ),
            )

        # Add PDF Failure Cleanup widgets to dashboard
        dashboard.add_widgets(
            cloudwatch.LogQueryWidget(
                title="Pipeline Failure Cleanup Activity",
                log_group_names=[pdf_cleanup_log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @message like /PIPELINE_FAILURE_CLEANUP/
                    | display @timestamp, @message
                    | sort @timestamp desc
                    | limit 50''',
                width=24,
                height=6
            ),
            cloudwatch.LogQueryWidget(
                title="Failures by User (Today)",
                log_group_names=[pdf_cleanup_log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @message like /PIPELINE_FAILURE_CLEANUP/
                    | filter @message like /uploaded_by/
                    | display @timestamp, @message
                    | sort @timestamp desc
                    | limit 50''',
                width=12,
                height=6
            ),
            cloudwatch.LogQueryWidget(
                title="Failure Reasons Summary",
                log_group_names=[pdf_cleanup_log_group_name],
                query_string='''fields @timestamp, @message
                    | filter @message like /PIPELINE_FAILURE_CLEANUP/
                    | display @timestamp, @message
                    | sort @timestamp desc
                    | limit 50''',
                width=12,
                height=6
            ),
        )


app = cdk.App()
PDFAccessibility(app, "PDFAccessibility")
app.synth()
