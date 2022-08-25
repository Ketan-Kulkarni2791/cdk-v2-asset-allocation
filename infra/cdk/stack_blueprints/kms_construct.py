"""Module for creating KMS encryption key"""
from typing import List
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

    @staticmethod
    def get_kms_key_encrypt_decrypt_policy(
            kms_keys: List[str]) -> iam.PolicyStatement:
        """Returns policy statement for encrypting and decrypting kms keys."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("kms:Decrept")
        policy_statement.add_actions("kms:Encrypt")
        policy_statement.add_actions("kms:ReEncrypt*")
        policy_statement.add_actions("kms:GenerateDataKey*")
        policy_statement.add_actions("kms:DescribeKey")
        for key in kms_keys:
            policy_statement.add_resources(key)
        return policy_statement