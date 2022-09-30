"""Glue Construct Testing"""
import unittest
from unittest.mock import patch, MagicMock, call

import aws_cdk.aws_iam as iam

from infra.cdk.stack_blueprints.glue_construct import GlueConstruct


class TestGlueConstruct(unittest.TestCase):
    """Glue construct testing class"""

    def setUp(self):

        iam.PolicyStatement = MagicMock()
        iam.PolicyStatement.return_value.add_actions = MagicMock()
        iam.PolicyStatement.return_value.add_resources = MagicMock()

        self.config = {
            'env': 'test',
            'catalogArn': 'test_catalog',
            'glueDatabaseArn': 'test_database_arn',
            'datasetTableArn': 'test_dataset_table_arn'
        }
        self.high_level_config = {
            'test': self.config,
            'global': {'source_id_short': 'testing-id-short'}
        }


    def test_get_glue_policy(self):
        action_calls=[
            call("glue:BatchCreatePartition"),
            call("glue:BatchGetPartition"),
            call("glue:CreateDatabase"),
            call("glue:CreatePartition"),
            call("glue:CreateTable"),
            call("glue:GetDatabase"),
            call("glue:GetDatabases"),
            call("glue:GetJob"),
            call("glue:GetPartition"),
            call("glue:GetPartitions"),
            call("glue:GetTable"),
            call("glue:GetTables"),
            call("glue:UpdatePartition"),
            call("glue:UpdateTable")
        ]
        resources_calls=[
            call(self.config['catalogArn']),
            call(self.config['glueDatabaseArn']),
            call(self.config['datasetTableArn']),
        ]
        GlueConstruct.get_glue_policy(self.high_level_config, self.config['env'])

        iam.PolicyStatement.assert_called_once_with()
        iam.PolicyStatement.return_value_add_actions.assert_has_calls(action_calls, any_order=True)
        iam.PolicyStatement.return_value_add_resources.assert_has_calls(resources_calls, any_order=True)