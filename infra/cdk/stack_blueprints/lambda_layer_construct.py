"""Code for generating lambda layers."""
from typing import List
import aws_cdk
import aws_cdk.aws_lambda as _lambda


class LambdaLayerConstruct:
    """Construct for creating lambdas layers."""

    @staticmethod
    def create_lambda_layer(
        stack: aws_cdk.Stack,
        config: dict,
        layer_name: str,
        compatible_runtimes: List[_lambda.Runtime]
    ) -> _lambda.LayerVersion:
        """Method to create lambda layers."""
        code_location = _lambda.AssetCode(config['global'][f"{layer_name}_location"])
        return _lambda.LayerVersion(
            scope=stack,
            id=f"{config['global']['appNameShort']}-{layer_name}-Id",
            code=code_location,
            compatible_runtimes=compatible_runtimes
        )