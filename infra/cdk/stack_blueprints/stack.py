"""Main python file_key for adding resources to the application stack."""
from typing import Dict, Any
import aws_cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
from constructs import Construct

from .iam_construct import IAMConstruct
from .kms_construct import KMSConstruct
from .s3_construct import S3Construct
from .lambda_construct import LambdaConstruct
from .lambda_layer_construct import LambdaLayerConstruct


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

        # IAM Role Setup --------------------------------------------------------
        stack_role = MainProjectStack.create_stack_role(
            config=config,
            stack=stack,
            kms_key=kms_key
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
            layer=layer
        )

        # S3 Bucket Infra Setup --------------------------------------------------
        MainProjectStack.create_bucket(
            config=config,
            env=env,
            stack=stack,
            function=lambdas["validation_trigger_lambda"]
        )

    @staticmethod
    def create_stack_role(
        config: dict,
        stack: aws_cdk.Stack,
        kms_key: kms.Key
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
        layers["requirement_layer"] = LambdaLayerConstruct.create_lambda_layer(
            stack=stack,
            config=config,
            layer_name="requirement_layer",
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
            layer: Dict[str, _lambda.LayerVersion] = None) -> Dict[str, _lambda.Function]:
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
                KMSConstruct.get_kms_key_encrypt_decrypt_policy([kms_key.key_arn])
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
            layer=[layer["requirement_layer"]],
            memory_size=3008
        )

        # Placeholder Lambda 1. ----------------------------------------------------
        pl_1_lambda_policy = IAMConstruct.create_managed_policy(
            stack=stack,
            config=config,
            policy_name="pl_1_lambda",
            statements=[
                LambdaConstruct.get_cloudwatch_policy(
                    config['global']['pl_1_lambdaLogsArn']
                )
            ]
        )
        
        pl_1_role = IAMConstruct.create_role(
            stack=stack,
            config=config,
            role_name="pl_1_lambda",
            assumed_by=['sqs', 'lambda', 'sns']
        )
        pl_1_role.add_managed_policy(pl_1_lambda_policy)

        lambdas["pl_1_lambda"] = LambdaConstruct.create_lambda(
            stack=stack,
            config=config,
            lambda_name="pl_1_lambda",
            role=pl_1_role,
            layer=None,
            memory_size=3008
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