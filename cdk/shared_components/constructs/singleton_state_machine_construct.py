from aws_cdk import Duration, Fn, aws_lambda
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class SingletonStateMachineConstruct(Construct):
    """
    A CDK construct that returns a step function with a built-in guard to prevent concurrent executions.
    The step function will invoke a lambda function to check if there are any running executions of the same state machine, and only proceed if there are none.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    id : str
        The construct ID
    step_function_name : str
        The name of the step function
    main_tasks : sfn.IChainable
        The main tasks of the step function
    timeout_minutes : int
        The timeout for the step function in minutes (default: 10)
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        step_function_name: str,
        main_tasks: sfn.IChainable,
        timeout_minutes: int = 10,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.check_step_function_running = Fn.import_value(
            "CheckStepFunctionRunningArnOutput"
        )
        self.check_step_function_running_function = (
            aws_lambda.Function.from_function_arn(
                self,
                "CheckStepFunctionRunningArnOutput",
                self.check_step_function_running,
            )
        )

        should_run_decision = self.make_should_run_decision(main_tasks)

        check_step_function_running_task = (
            self.make_check_step_function_running_task()
        )
        state_definition = check_step_function_running_task.next(
            should_run_decision
        )

        step_function = sfn.StateMachine(
            self,
            step_function_name,
            state_machine_name=step_function_name,
            definition=state_definition,
            timeout=Duration.minutes(timeout_minutes),
        )

        self.entry_point = step_function

    def make_check_step_function_running_task(self) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "CheckConcurrentExecution",
            lambda_function=self.check_step_function_running_function,
            payload=sfn.TaskInput.from_object(
                {
                    "stateMachineArn.$": "$$.StateMachine.Id",
                    "currentExecutionArn.$": "$$.Execution.Id",
                }
            ),
            output_path="$.Payload",
        )

    def make_should_run_decision(self, main_tasks) -> sfn.Choice:
        fail_state = sfn.Fail(
            self,
            "StopExecution",
            cause="ConcurrentExecution",
            error="AnotherExecutionRunning",
        )
        decision = sfn.Choice(self, "CanProceed?")
        decision.when(
            sfn.Condition.boolean_equals("$.proceed", True), main_tasks
        )
        decision.otherwise(fail_state)
        return decision
