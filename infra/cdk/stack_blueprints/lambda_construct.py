"""Code for generation and deploy lambda to AWS"""
import json
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as aws_lambda
import aws_cdk.aws_logs as aws_logs
from aws_cdk import Stack, Duration


class LambdaConstruct:
    """Class with static methods that are used to build and deploy lambdas."""

    @staticmethod
    def create_lambda(
            stack: Stack,
            config: dict,
            # env: str,
            lambda_name: str,
            role: iam.Role,
            duration: Duration = None) -> aws_lambda.Function:
        """Method called by construct for creating lambda."""

        return LambdaConstruct.create_lambda_function(
            stack=stack,
            config=config,
            lambda_name=lambda_name,
            role=role,
            duration=duration
        )

    @staticmethod
    def create_lambda_function(
            stack: Stack,
            config: dict,
            lambda_name: str,
            role: iam.Role,
            duration: Duration) -> aws_lambda.Function:
        """Methods for generic lambda creation."""

        env_vars = json.loads(config['global'][f"{lambda_name}Environment"])
        lambda_path = config['global'][f"{lambda_name}HndlrPath"]
        handler = config['global'][f"{lambda_name}Hndlr"]
        function_id = f"{config['global']['app-name']}-{lambda_name}-Id"
        function_name = f"{config['global']['app-name']}-{lambda_name}"

        dict_props = {
            "function_name": function_name,
            "code": aws_lambda.Code.from_asset(path=lambda_path),
            "handler": handler,
            "runtime": aws_lambda.Runtime.PYTHON_3_8,
            "role": role,
            "environment": env_vars,
            "architecture": aws_lambda.Architecture.ARM_64,
            "timeout": duration,
            "log_retention": aws_logs.RetentionDays.THREE_MONTHS
        }

        return aws_lambda.Function(scope=stack, id=function_id, **dict_props)        

    @staticmethod
    def get_cloudwatch_policy(lambda_logs_arn: str) -> iam.PolicyStatement:
        """Returns policy statement for creating logs."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("cloudwatch:PutMetricData")
        policy_statement.add_actions("logs:CreateLogGroup")
        policy_statement.add_actions("logs:CreateLogStream")
        policy_statement.add_actions("logs:GetQueryResults")
        policy_statement.add_actions("logs:PutLogEvents")
        policy_statement.add_actions("logs:StartQuery")
        policy_statement.add_resources(lambda_logs_arn)
        return policy_statement

    @staticmethod
    def get_sfn_execute_policy(step_function_arn: str) -> iam.PolicyStatement:
        """Returns policy statements for executing step function."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("states:StartExecution")
        policy_statement.add_resources(step_function_arn)
        return policy_statement