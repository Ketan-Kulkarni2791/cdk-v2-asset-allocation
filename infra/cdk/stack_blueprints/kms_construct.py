"""Module for creating KMS encryption key"""
# from typing import List
from aws_cdk import Stack
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms 


class KMSConstruct:
    """Class for methods to create KMS keys"""

    @staticmethod
    def create_kms_key(
            stack: Stack,
            config: dict,
            policy_doc: iam.PolicyDocument) -> kms.Key:
        """Create KMS key for encrypting AWS resources (s3, SNS, etc)."""
        return kms.Key(
            scope=stack,
            id=f"{config['global']['app-name']}-keyId",
            alias=f"{config['global']['app-name']}-kms",
            enabled=True,
            policy=policy_doc
        )