from aws_cdk import (
    aws_lambda as lambda_,
)
from aws_cdk import (
    aws_stepfunctions as sfn,
)
from aws_cdk import (
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class AddressBaseSourceCheckConstruct(Construct):
    """
    A CDK construct that checks if there is exactly one distinct addressbase source.

    This construct creates a three-step workflow:
    1. Run an Athena query to count distinct addressbase sources
    2. Get the query results
    3. Check if there's exactly one source, succeeding if yes, fail if no.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        The Lambda function that will execute Athena queries
    table_name : str
        The table name to check against. Must have a column named 'addressbase_source'
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        table_name,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # State names must be unique within a state machine, so they are
        # namespaced by construct_id to allow more than one instance of this
        # construct to be used in the same state machine.
        count_distinct_addressbase_source = tasks.LambdaInvoke(
            self,
            f"{construct_id}: Count distinct addressbase sources",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {"table_name": table_name},
                    "QueryString": "SELECT COUNT(DISTINCT addressbase_source) AS addressbase_sources_count FROM {table_name};",
                    "blocking": True,
                }
            ),
        )

        # Create the state to get Athena query results
        get_addressbase_source_count = tasks.AthenaGetQueryResults(
            self,
            f"{construct_id}: Get addressbase sources count",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                "addressbase_source_count": "{% $states.result.ResultSet.Rows[1].Data[0].VarCharValue %}"
            },
        )

        # Create the state to check if there's exactly one addressbase source
        check_addressbase_source_count = (
            sfn.Choice(self, f"{construct_id}: Check addressbase source count")
            .when(
                sfn.Condition.string_equals(
                    "$addressbase_source_count",
                    "1",
                ),
                sfn.Succeed(
                    self,
                    f"{construct_id}: Only one source of addresssbase!",
                ),
            )
            .otherwise(
                sfn.Fail(
                    self, f"{construct_id}: Not one source of addresssbase!"
                )
            )
        )

        # Chain the states together
        count_distinct_addressbase_source.next(
            get_addressbase_source_count
        ).next(check_addressbase_source_count)

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = count_distinct_addressbase_source
