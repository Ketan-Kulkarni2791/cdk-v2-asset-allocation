"""KMS Construct Testing."""
import unittest
from unittest.mock import MagicMock, patch, Mock, call
import aws_cdk.aws_iam as iam

from infra.cdk.stack_blueprints.kms_construct import KMSConstruct


class TestKMSConstruct(unittest.TestCase):
    """KMS Construct testing class."""

    def setUp(self) -> None:
        self.addCleanup(patch.stopall)
        self.mocked_stack = Mock()
        
        self.mock_kms_key = patch("aws_cdk.aws_kms.Key", spec=True).start()
        self.mock_kms_key_from_key_arn = patch(
            "aws_cdk.aws_kms.Key.from_key_arn", spec=True
        ).start()
        self.mock_policy_doc = patch(
            "aws_cdk.aws_iam.PolicyDocument", spec=True
        ).start()

        iam.PolicyStatement = MagicMock()
        iam.PolicyStatement.return_value.add_actions = MagicMock()
        iam.PolicyStatement.return_value.add_resources = MagicMock()

        self.config = {
            "app-name": "test-app-name",
            "env": "test"
        }

        self.high_level_config = {"global": self.config}

    def test_create_kms_key(self) -> None:
        KMSConstruct.create_kms_key(
            self.mocked_stack,
            self.high_level_config,
            self.mock_policy_doc
        )

        self.mock_kms_key.assert_called_once_with(
            scope=self.mocked_stack,
            id=f"{self.config['app-name']}-keyId",
            alias=f"{self.config['app-name']}-kms",
            enabled=True,
            policy=self.mock_policy_doc
        )

    def test_get_kms_key_encrypt_decrypt_policy():
        action_call = [
            call("kms:Decrept"),
            call("kms:Encrypt"),
            call("kms:ReEncrypt*"),
            call("kms:GenerateDataKey*"),
            call("kms:DescribeKey")
        ]
        resources_calls = [call("fake_key_0"), call("fake_key_1")]

        KMSConstruct.get_kms_key_encrypt_decrypt_policy(["fake_key_0", "fake_key_1"])

        iam.PolicyStatement.assert_called_once_with()
        iam.PolicyStatement.return_value.add_actions.assert_has_calls(action_call, any_order=True)
        iam.PolicyStatement.return_value.add_resources.assert_has_calls(resources_calls, any_order=True)

        
