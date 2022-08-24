#!/usr/bin/env python3
from configparser import ConfigParser, ExtendedInterpolation
import aws_cdk as cdk
from stack_blueprints.stack import MainProjectStack


def main() -> None:
    """main.py method that the cdk will execute."""
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read("../../.configrc/config.ini")
    app = cdk.App()
    env = app.node.try_get_context("env")
    MainProjectStack(
        env_var=env,
        scope=app,
        app_id=config['global']['app-id'],
        config=config,
        env={
            "region": config['global']["region"],
            "account": config['global']['awsAccount']
        }
    )
    app.synth()


main()