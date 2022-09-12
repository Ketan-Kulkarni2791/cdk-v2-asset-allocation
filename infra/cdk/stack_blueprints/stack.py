"""Main python file_key for adding resources to the application stack."""
from typing import Dict, Any
import aws_cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
from constructs import Construct

from .iam_construct import IAMConstruct
from .kms_construct import KMSConstruct
from .s3_construct import S3Construct
from .lambda_construct import LambdaConstruct
from .lambda_layer_construct import LambdaLayerConstruct
from .sns_construct import SNSConstruct
from .stepfunction_construct import StepFunctionConstruct
from .glue_construct import GlueConstruct


class MainProjectStack(aws_cdk.Stack):
    """Build the app stacks and its resources."""
    def __init__(self, env_var: str, scope: Construct, 
                 app_id: str, config: dict, **kwargs: Dict[str, Any]) -> None:
        """Creates the cloudformation templates for the projects."""
        super().__init__(scope, app_id, **kwargs)
        self.env_var = env_var
        self.config = config
        MainProjectStack.create_stack(self, config=self.config, env=self.env_var)

    @staticmethod
    def create_stack(
            stack: aws_cdk.Stack,  
            config: dict,
            env: str) -> None:
        """Create and add the resources to the application stack"""

        # KMS infra setup ------------------------------------------------------
        kms_pol_doc = IAMConstruct.get_kms_policy_document()

        kms_key = KMSConstruct.create_kms_key(
            stack=stack,
            config=config,
            policy_doc=kms_pol_doc
        )
        print(kms_key)

        # SNS Infra Setup -----------------------------------------------------
        sns_topic = MainProjectStack.setup_sns_topic(
            config,
            kms_key,
            stack
        )

        # IAM Role Setup --------------------------------------------------------
        stack_role = MainProjectStack.create_stack_role(
            config=config,
            stack=stack,
            kms_key=kms_key,
            sns_topic=sns_topic
        )
        print(stack_role)

        # Lambda Layers --------------------------------------------------------
        layer = MainProjectStack.create_layers_for_lambdas(
            stack=stack,
            config=config
        )

        # Infra for Lambda function creation -------------------------------------
        lambdas = MainProjectStack.create_lambda_functions(
            stack=stack,
            config=config,
            # env=env,
            kms_key=kms_key,
            layers=layer,
            sns_topic=sns_topic
        )

        # S3 Bucket Infra Setup --------------------------------------------------
        MainProjectStack.create_bucket(
            config=config,
            env=env,
            stack=stack,
            function=lambdas["validation_trigger_lambda"]
        )

        # Step Function Infra Creation -------------------------------------------
        MainProjectStack.create_step_function(
            stack=stack,
            config=config,
            # env=env,
            kms_key=kms_key,
            lambdas=lambdas,
            sns_topic=sns_topic
        )

    @staticmethod
    def setup_sns_topic(
            config: dict,
            kms_key: kms.Key,
            stack: aws_cdk.Stack) -> sns.Topic:
        """Set up the SNS Topic and returns the SNS Topic Object."""
        sns_topic = SNSConstruct.create_sns_topic(
            stack=stack,
            config=config,
            kms_key=kms_key
        )
        SNSConstruct.subscribe_email(config=config, topic=sns_topic)
        return sns_topic

    @staticmethod
    def create_stack_role(
        config: dict,
        stack: aws_cdk.Stack,
        kms_key: kms.Key,
        sns_topic: sns.Topic
    ) -> iam.Role:
        """Create the IAM role."""

        stack_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="mainStack",
            statements=[
                KMSConstruct.get_kms_key_encrypt_decrypt_policy(
                    [kms_key.key_arn]
                ),
                S3Construct.get_s3_object_policy([config['global']['bucket_arn']]),
                SNSConstruct.get_sns_publish_policy(sns_topic.topic_arn)
            ]
        )
        stack_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="mainStack",
            assumed_by=["s3", "lambda"]
        )
        stack_role.add_managed_policy(policy=stack_policy)
        return stack_role

    @staticmethod
    def create_layers_for_lambdas(
            stack: aws_cdk.Stack,
            config: dict) -> Dict[str, _lambda.LayerVersion]:
        """Method to create layers."""
        layers = {}
        # requirement layer for general ----------------------------------------------------
        layers["pandas"] = LambdaLayerConstruct.create_lambda_layer(
            stack=stack,
            config=config,
            layer_name="pandas_layer",
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_8
            ]
        )
        layers["psycopg2"] = LambdaLayerConstruct.create_lambda_layer(
            stack=stack,
            config=config,
            layer_name="psycopg2_layer",
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_8
            ]
        )
        return layers

    @staticmethod
    def create_lambda_functions(
            stack: aws_cdk.Stack,
            config: dict,
            kms_key: kms.Key,
            sns_topic: sns.Topic,
            layers: Dict[str, _lambda.LayerVersion] = None) -> Dict[str, _lambda.Function]:
        """Create placeholder lambda function and roles."""

        lambdas = {}

        # Validation Trigger Lambda. ----------------------------------------------------
        validation_trigger_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="validation_trigger",
            statements=[
                LambdaConstruct.get_sfn_execute_policy(
                    config['global']['stepFunctionArn']  
                ),
                LambdaConstruct.get_cloudwatch_policy(
                    config['global']['validation_trigger_lambdaLogsArn']
                ),
                S3Construct.get_s3_object_policy(config['global']['bucket_arn']),
                S3Construct.get_s3_bucket_policy(config['global']['bucket_arn']),
                KMSConstruct.get_kms_key_encrypt_decrypt_policy([kms_key.key_arn]),
                SNSConstruct.get_sns_publish_policy(sns_topic.topic_arn)
            ]
        )

        validation_trigger_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="validation_trigger",
            assumed_by=["lambda", "s3"]   
        )

        validation_trigger_role.add_managed_policy(validation_trigger_policy)

        lambdas["validation_trigger_lambda"] = LambdaConstruct.create_lambda(
            stack=stack,
            config=config,
            lambda_name="validation_trigger_lambda",
            role=validation_trigger_role,
            sns_arn=sns_topic.topic_arn,
            layer=[layers["pandas"]],
            memory_size=3008
        )

        # Placeholder Lambda 1. ----------------------------------------------------
        infra_check_lambda_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="infra_check_lambda",
            statements=[
                LambdaConstruct.get_cloudwatch_policy(
                    config['global']['infra_check_lambdaLogsArn']
                ),
                S3Construct.get_s3_object_policy(config["global"]["bucket_arn"]),
                LambdaConstruct.get_ec2_policy(),
                # LambdaConstruct.get_redshift_policy(config=config),
                GlueConstruct.get_glue_policy(config),
                KMSConstruct.get_kms_key_encrypt_decrypt_policy([kms_key.key_arn]),
                SNSConstruct.get_sns_publish_policy(sns_topic.topic_arn)
            ]
        )

        infra_check_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="infra_check_lambda",
            assumed_by=['lambda', 'sns']
        )
        infra_check_role.add_managed_policy(infra_check_lambda_policy)

        lambdas["infra_check_lambda"] = LambdaConstruct.create_lambda(
            stack=stack,
            config=config,
            lambda_name="infra_check_lambda",
            role=infra_check_role,
            layer=[layers["psycopg2"]],
            memory_size=3008,
            duration=aws_cdk.Duration.minutes(amount=15)
        )

        # Placeholder Lambda 2. ----------------------------------------------------
        pl_2_lambda_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="pl_2_lambda",
            statements=[
                LambdaConstruct.get_cloudwatch_policy(
                    config['global']['pl_2_lambdaLogsArn']
                )
            ]
        )

        pl_2_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="pl_2_lambda",
            assumed_by=['sqs', 'lambda', 'sns']
        )
        pl_2_role.add_managed_policy(pl_2_lambda_policy)

        lambdas["pl_2_lambda"] = LambdaConstruct.create_lambda(
            stack=stack,
            config=config,
            lambda_name="pl_2_lambda",
            role=pl_2_role,
            layer=None,
            memory_size=3008
        )

        # Clear Files on Alert Lambda. ----------------------------------------------------
        clear_files_on_alert_lambda_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="alert_lambda",
            statements=[
                LambdaConstruct.get_cloudwatch_policy(
                    config["global"]["clearFileslambdaLogsArn"]
                ),
                KMSConstruct.get_kms_key_encrypt_decrypt_policy([kms_key.key_arn]),
                SNSConstruct.get_sns_publish_policy(sns_topic.topic_arn),
                S3Construct.get_s3_bucket_policy(
                    [config["global"]["bucket_arn"]]
                ),
                S3Construct.get_s3_object_policy(
                    [config["global"]["bucket_arn"]]
                )
            ]
        )
        clear_files_on_alert_lambda_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="alert_lambda",
            assumed_by=['sqs', 'lambda']
        )
        clear_files_on_alert_lambda_role.add_managed_policy(
            clear_files_on_alert_lambda_policy
        )
        lambdas["clear_files_alert_lambda"] = LambdaConstruct.create_lambda(
            stack=stack,
            config=config,
            lambda_name="clearFilesLambda",
            role=clear_files_on_alert_lambda_role,
            sns_arn=sns_topic.topic_arn
        )

        return lambdas

    @staticmethod
    def create_bucket(
            config: dict,
            env: str,
            stack: aws_cdk.Stack,
            function: Dict[str, _lambda.Function]) -> s3.Bucket:
        """Create an encrypted s3 bucket."""

        print(env)
        s3_bucket = S3Construct.create_bucket(
            stack=stack,
            bucket_id=f"asset-allocation-{config['global']['env']}",
            bucket_name=config['global']['bucket_name']
        )

        S3Construct.create_lambda_trigger(
            bucket=s3_bucket,
            prefix=config["global"]["triggerPrefix"],
            suffix=config["global"]["triggerSuffix"],
            function=function,
            event_type=s3.EventType.OBJECT_CREATED
        )

    @staticmethod
    def create_step_function(
            stack: aws_cdk.Stack,
            config: dict,
            sns_topic: sns.Topic,
            kms_key: kms.Key,
            lambdas: Dict[str, _lambda.Function]) -> None:
        """Create step function and necessary IAM role with input lambda."""

        state_machine_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="stateMachine",
            statements=[
                StepFunctionConstruct.get_sfn_lambda_invoke_job_policy_statement(config),
                KMSConstruct.get_kms_key_encrypt_decrypt_policy([kms_key.key_arn]),
                SNSConstruct.get_sns_publish_policy(sns_topic.topic_arn)
            ]
        )
        state_machine_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="stateMachine",
            assumed_by=['states']
        )
        state_machine_role.add_managed_policy(state_machine_policy)

        StepFunctionConstruct.create_step_function(
            stack=stack,
            config=config,
            role=state_machine_role,
            infra_check_lambda=lambdas["infra_check_lambda"],
            pl_2_lambda=lambdas["pl_2_lambda"],
            clear_files_alert_lambda=lambdas["clear_files_alert_lambda"],
            sns_topic=sns_topic
        )      