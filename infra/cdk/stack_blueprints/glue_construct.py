"""Create Glue resources, and its related tasks, triggers, jobs and workflows."""
import aws_cdk.aws_iam as iam


class GlueConstruct:
    """Create Glue resources, and any related tasks, triggers, jobs and workflows."""

    @staticmethod
    def get_glue_policy(config: dict) -> iam.PolicyDocument:
        """Returns policy statement for managing Glue resources and components."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("glue:BatchCreatePartition")
        policy_statement.add_actions("glue:BatchDeleteTable")
        policy_statement.add_actions("glue:BatchGetPartition")
        policy_statement.add_actions("glue:CreateDatabase")
        policy_statement.add_actions("glue:CreatePartition")
        policy_statement.add_actions("glue:CreateTable")
        policy_statement.add_actions("glue:DeleteTable")
        policy_statement.add_actions("glue:GetDatabase")
        policy_statement.add_actions("glue:GetDatabases")
        policy_statement.add_actions("glue:GetJob")
        policy_statement.add_actions("glue:GetPartition")
        policy_statement.add_actions("glue:GetPartitions")
        policy_statement.add_actions("glue:GetTable")
        policy_statement.add_actions("glue:GetTables")
        policy_statement.add_actions("glue:UpdatePartition")
        policy_statement.add_actions("glue:UpdateTable")
        policy_statement.add_resources(config['global']['catalogArn'])
        policy_statement.add_resources(config['global']['glueDatabaseArn'])
        policy_statement.add_resources(config['global']['datasetTableArn'])
        
        return policy_statement