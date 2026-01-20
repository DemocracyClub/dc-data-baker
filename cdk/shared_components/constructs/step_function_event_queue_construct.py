import aws_cdk.aws_pipes_alpha as aws_pipes_alpha
import aws_cdk.aws_pipes_sources_alpha as aws_pipes_sources_alpha
import aws_cdk.aws_pipes_targets_alpha as aws_pipes_targets_alpha
from aws_cdk import Duration
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct


class StepFunctionEventQueueConstruct(Construct):
    """
    A CDK construct that creates a FIFO SQS queue with a pipe to trigger a given step function.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    id : str
        The construct ID
    queue_name : str
        The name of the SQS queue to create
    pipe_name : str
        The name of the pipe to create
    target_step_function : sfn.IStateMachine
        The target step function to trigger
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        queue_name: str,
        pipe_name: str,
        target_step_function: sfn.IStateMachine,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Create the event queue
        event_queue = sqs.Queue(
            self,
            queue_name,
            fifo=True,
            content_based_deduplication=True,
            queue_name=f"{queue_name}.fifo",
            encryption=sqs.QueueEncryption.UNENCRYPTED,
            delivery_delay=Duration.minutes(5),
        )

        # Create the pipe
        pipe_role = iam.Role(
            self,
            "PipeRole",
            assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
        )

        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                ],
                resources=[event_queue.queue_arn],
            )
        )

        pipe_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[target_step_function.state_machine_arn],
            )
        )

        aws_pipes_alpha.Pipe(
            self,
            pipe_name,
            role=pipe_role,
            source=aws_pipes_sources_alpha.SqsSource(
                event_queue,
            ),
            target=aws_pipes_targets_alpha.SfnStateMachine(
                target_step_function,
                invocation_type=aws_pipes_targets_alpha.StateMachineInvocationType.FIRE_AND_FORGET,
            ),
        )

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = event_queue
