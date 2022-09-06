"""Code for generating the SNS Topic. Also used to add a email subscription to a topic."""
from aws_cdk import Stack
import aws_cdk.aws_kms as kms
import aws_cdk.aws_iam as iam
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subcriptions as aws_sns_subcriptions


class SNSConstruct:
    """Class with static methods that are used to build and deploy SNS Topic."""

    @staticmethod
    def create_sns_topic(
        stack: Stack,
        config: dict,
        kms_key: kms.Key) -> sns.Topic:
        """Creates SNS Topic and adds to the input stack."""
        return sns.Topic(
            scope=stack,
            id=f"{config['global']['app-name']}-sns-topic",
            display_name=f"{config['global']['source-id-short']} Reservoir Topic",
            master_key=kms_key
        )

    @staticmethod
    def subscribe_email(config: dict, topic: sns.Topic) -> None:
        """Creates SNS email subscription to the input topic."""
        topic.add_subscription(
            aws_sns_subcriptions.EmailSubscription(
                email_address=config['global']['email']
            )
        )

    @staticmethod
    def get_sns_publish_policy(sns_topic_arn: str) -> iam.PolicyStatement:
        """Returns Policy Statement for publishing to an sns topic."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("sns:Publish")
        policy_statement.add_resources(sns_topic_arn)
        return policy_statement