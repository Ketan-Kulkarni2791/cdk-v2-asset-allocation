"""Main python file_key for adding resources to the application stack."""
from typing import Dict, Any
import aws_cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
from constructs import Construct

from .iam_construct import IAMConstruct
from .kms_construct import KMSConstruct
from .s3_construct import S3Construct


class MainProjectStack(aws_cdk.Stack):
    """Build the app stacks and its resources."""
    def __init__(self, env_var: str, scope: Construct, 
                 app_id: str, config: dict, **kwargs: Dict[str, Any]) -> None:
        """Creates the cloudformation templates for the projects."""
        super().__init__(scope, app_id, **kwargs)
        self.env_var = env_var
        self.config = config
        MainProjectStack.create_stack(self, config=self.config)

    @staticmethod
    def create_stack(
            stack: aws_cdk.Stack,  
            config: dict) -> None:
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
        