"""Code for generation and deployment of s3 resources."""
# from aws_cdk import Stack, Duration
import aws_cdk.aws_iam as iam
# import aws_cdk.aws_s3 as s3


class S3Construct:
    """Class with static methods that are used to build and deploy s3."""

    @staticmethod
    def get_s3_object_policy(s3_bucket_arns: str) -> iam.PolicyStatement:
        """Returns policy statement for reading and writing S3 objects."""

        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("s3:DeleteObject*")
        policy_statement.add_actions("s3:GetObject*")
        policy_statement.add_actions("s3:PutObject*")
        # policy_statement.add_actions("s3:ReplicateTags")
        policy_statement.add_actions("s3:ListBucket")
        policy_statement.add_actions("s3:GetBucketLocation")
        policy_statement.add_actions("s3:AbortMultipartUpload")
        policy_statement.add_actions("s3:ListBucketMultipartUploads")
        policy_statement.add_actions("s3:ListMultipartUploadParts")
        policy_statement.add_actions("s3:GetBucket*")
        policy_statement.add_resources(f"{s3_bucket_arns}")
        policy_statement.add_resources(f"{s3_bucket_arns}/*")
        return policy_statement

    @staticmethod
    def get_s3_bucket_policy(s3_bucket_arns: str) -> iam.PolicyStatement:
        """Returns policy statement for reading from S3 bucket."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("s3:GetBucket*")
        policy_statement.add_actions("s3:PutBucket")
        policy_statement.add_actions("s3:ListBucket")
        policy_statement.add_actions("s3:GetBucketLocation")
        policy_statement.add_actions("s3:GetObject")
        policy_statement.add_resources(s3_bucket_arns)
        return policy_statement