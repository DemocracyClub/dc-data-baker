from aws_cdk import (
    aws_lambda as lambda_,
)
from aws_cdk import (
    aws_stepfunctions as sfn,
)
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class MakePartitionsConstruct(Construct):
    """
    A CDK construct that creates partitions on Athena for a given table.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        The Lambda function that will execute Athena queries
    target_table_name : str
        The target table to partition
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        target_table_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        make_partitions = tasks.LambdaInvoke(
            self,
            f"Make partitions for {target_table_name}",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {"table_name": target_table_name},
                    "QueryString": "MSCK REPAIR TABLE `{table_name}`;",
                    "blocking": True,
                }
            ),
        )

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = make_partitions
