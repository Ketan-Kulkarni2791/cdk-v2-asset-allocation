"""Code for generating Stepfunction resources, including tasks, retries, errors etc."""
import aws_cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_sns as sns
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as sfn_tasks
import aws_cdk.aws_logs as aws_logs


class StepFunctionConstruct:
    """Class has methods to create a step function."""

    @staticmethod
    def create_step_function(
            stack: aws_cdk.Stack,
            config: dict,
            role: iam.Role,
            pl_1_lambda: _lambda.Function,
            pl_2_lambda: _lambda.Function,
            clear_files_alert_lambda: _lambda.Function,
            sns_topic: sns.Topic) -> sfn.StateMachine:
        """Create Step Function for Asset Allocation Data Load."""

        # Step function's failure, success and choice state ----------------------------------
        succeeded = StepFunctionConstruct.create_succeeded_state(
            stack=stack,
            config=config
        )

        failure = StepFunctionConstruct.create_fail_state(
            stack=stack,
            config=config
        )

        # Red Alert Task --------------------------------------------------------------------
        red_alert = StepFunctionConstruct.create_alert_task(
            stack=stack,
            sns_topic=sns_topic,
            subject=f"Error-{config['global']['source-id-short']} Step Function"
        )

        # Clear Files on Red Alert ---------------------------------------------------------
        clear_files_on_alert_lambda_task = StepFunctionConstruct.create_lambda_task(
            stack=stack,
            task_def="Clear Files on Alert Lambda",
            task_lambda=clear_files_alert_lambda,
            result_key="$.output"
        )

        red_alert.next(clear_files_on_alert_lambda_task).next(failure)

        # Lambda 1 ------------------------------------------------------------------------
        pl_1_lambda_task = StepFunctionConstruct.create_lambda_task(
            stack=stack,
            task_def="Placeholder Lambda 1",
            task_lambda=pl_1_lambda,
            result_key="$.output"
        )
        pl_1_lambda_task.add_catch(red_alert, result_path="$.Error")

        # Lambda 2 ------------------------------------------------------------------------
        pl_2_lambda_task = StepFunctionConstruct.create_lambda_task(
            stack=stack,
            task_def="Placeholder Lambda 2",
            task_lambda=pl_2_lambda,
            result_key="$.output"
        )
        pl_2_lambda_task.add_catch(red_alert, result_path="$.Error")

        # StepFunction Definition --------------------------------------------------------
        definition = pl_1_lambda_task.next(pl_2_lambda_task).next(succeeded)

        # StepFunction Log Group ---------------------------------------------------------
        log_group = StepFunctionConstruct.create_sfn_log_group(
            stack=stack,
            config=config,
            log_group_name="stepFunction"
        )

        # Finally create a state machine -------------------------------------------------
        return sfn.StateMachine(
            scope=stack,
            id=f"{config['global']['appNameShort']}-stateMachine-Id",
            logs=sfn.LogOptions(
                destination=log_group,
                include_execution_data=True,
                level=sfn.LogLevel.ALL
            ),
            definition=definition,
            role=role,
            tracing_enabled=True
        )

    @staticmethod
    def create_succeeded_state(stack: aws_cdk.Stack, config: dict) -> sfn.Succeed:
        """Function to create succeeded state."""
        return sfn.Succeed(
            scope=stack,
            id=f"{config['global']['app-name']}-SucceedState-Id",
            comment="StepFunction Execution Successful"
        )

    @staticmethod
    def create_fail_state(stack: aws_cdk.Stack, config: dict) -> sfn.Fail:
        """Function to create fail state."""
        return sfn.Fail(
            scope=stack,
            id=f"{config['global']['app-name']}-FailState-Id",
            cause="An exception was thrown and not handled. Check email."
        )

    @staticmethod
    def create_alert_task(
            stack: aws_cdk.Stack,
            sns_topic: sns.Topic,
            subject: str) -> sfn_tasks.SnsPublish:
        """Function to create alert task."""
        return sfn_tasks.SnsPublish(
            scope=stack,
            id="RedAlert",
            topic=sns_topic,
            message=sfn.TaskInput.from_json_path_at('$.Error'),
            subject=subject,
            result_path="$.output"
        )

    @staticmethod
    def create_lambda_task(
            stack: aws_cdk.Stack,
            task_def: str,
            task_lambda: _lambda.Function,
            result_key: str = '$') -> sfn_tasks.LambdaInvoke:
        """Function to create lambda Task."""
        return sfn_tasks.LambdaInvoke(
            scope=stack,
            id=task_def,
            lambda_function=task_lambda,
            result_path=result_key
        )

    @staticmethod
    def create_sfn_log_group(
            stack: aws_cdk.Stack,
            config: dict,
            log_group_name: str) -> aws_logs.LogGroup:
        """Function to create log groups for StepFunction."""
        return aws_logs.LogGrou(
            scope=stack,
            id=f"{config['global']['app-name']}-LogGroup",
            log_group_name=f"{config['global']['app-name']}-{log_group_name}"
        )

    @staticmethod
    def get_sfn_lambda_invoke_job_policy_statement(config: dict) -> iam.PolicyStatement:
        """Returns policy statement Lambdas use for managing sfn resources and components."""
        policy_statement = iam.PolicyStatement()
        policy_statement.effect = iam.Effect.ALLOW
        policy_statement.add_actions("lambda:InvokeFunction")
        policy_statement.add_resources(config['global']['lambdaFunctionArnBase'])
        return policy_statement  