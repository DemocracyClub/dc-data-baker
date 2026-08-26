from aws_cdk import (
    aws_stepfunctions as sfn,
)
from constructs import Construct
from shared_components.constructs.singleton_state_machine_construct import (
    SingletonStateMachineConstruct,
)


class CoordinatedSingletonStateMachineConstruct(Construct):
    """
    A CDK construct that returns a singleton state machine with an added check to ensure that it is only executed by a coordinator stack.


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

        coordinated_stack_check = (
            sfn.Choice(self, "Is this being run by a coordinator stack?")
            .when(
                sfn.Condition.is_present("$.coordinated_stack"),
                sfn.Pass(self, "Yes, continue execution"),
            )
            .otherwise(
                sfn.Fail(
                    self,
                    f"{construct_id}: StopExecution",
                    cause="This state machine should only be run by a coordinator stack.",
                    error="NotRunByCoordinatorStack",
                )
            )
        )

        main_tasks_with_coordinator_check = (
            coordinated_stack_check.afterwards().next(main_tasks)
        )

        coordinated_singleton_state_machine = SingletonStateMachineConstruct(
            self,
            step_function_name,
            step_function_name=step_function_name,
            main_tasks=main_tasks_with_coordinator_check,
            timeout_minutes=timeout_minutes,
        ).entry_point

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = coordinated_singleton_state_machine
