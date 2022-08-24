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
            "appName": "test-app-name",
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
            id=f"{self.config['appName']}-keyId",
            alias=f"{self.config['appName']}-kms",
            enabled=True,
            policy=self.mock_policy_doc
        )