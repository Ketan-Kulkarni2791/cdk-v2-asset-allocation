"""Code for generation and deployment of s3 resources."""
from aws_cdk import Stack, Duration
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3_notifications as aws_s3_notifications


class S3Construct:
    """Class with static methods that are used to build and deploy s3."""

    @staticmethod
    def create_bucket(
            stack: Stack,
            bucket_id: str,
            bucket_name: str) -> s3.Bucket:
        """Creates an encrypted bucket."""

        bucket_metrics = s3.BucketMetrics(
            id=f"erm-{bucket_name}"
        )

        lifecycle_rule = s3.LifecycleRule(
            abort_incomplete_multipart_upload_after=Duration.days(1),
            enabled=True,
            noncurrent_version_expiration=Duration.days(1)
        )

        ga_bucket = s3.Bucket(
            scope=stack,
            id=bucket_id,
            access_control=s3.BucketAccessControl.PUBLIC_READ_WRITE,
            public_read_access=True,
            # block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_name=bucket_name,
            versioned=True,
            metrics=[bucket_metrics],
            lifecycle_rules=[lifecycle_rule]
        )

        return ga_bucket

    @staticmethod
    def create_lambda_trigger(
            bucket: s3.Bucket,
            prefix: str,
            suffix: str,
            function: _lambda.Function,
            event_type: s3.EventType) -> None:
        """Create trigger on lambda."""

        outbound_filters = s3.NotificationKeyFilter(
            prefix=prefix,
            suffix=suffix
        )

        lambda_dest = aws_s3_notifications.LambdaDestination(function)
        bucket.add_event_notification(event_type, lambda_dest, outbound_filters)

    @staticmethod
    def get_s3_object_policy(s3_bucket_arns: str) -> iam.PolicyStatement:
        """Returns policy statement for reading and writing S3 objects."""

        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("s3:DeleteObject*")
        policy_statement.add_actions("s3:GetObject*")
        policy_statement.add_actions("s3:PutObject*")
        policy_statement.add_actions("s3:ReplicateTags")
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