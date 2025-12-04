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


class RowCountCheckConstruct(Construct):
    """
    A CDK construct that checks if two tables have the same row count.

    This construct creates a three-step workflow:
    1. Run an Athena query to count rows in both tables
    2. Get the query results
    3. Check if the counts match, succeeding if yes, failing if no

    The construct is useful for data quality validation to ensure that
    transformations preserve all rows from the source table.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        The Lambda function that will execute Athena queries
    source_table_name : str
        The source table to count rows from
    target_table_name : str
        The target table to count rows from (should match source)
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        source_table_name: str,
        target_table_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Create the state to count rows in both tables
        count_rows_in_both_tables = tasks.LambdaInvoke(
            self,
            "Count rows in source and target tables",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "source_table_name": source_table_name,
                        "target_table_name": target_table_name,
                    },
                    "QueryString": """
                        WITH source_count AS (
                            SELECT COUNT(*) AS count
                            FROM {source_table_name}
                        ),
                        target_count AS (
                            SELECT COUNT(*) AS count
                            FROM {target_table_name}
                        )
                        SELECT
                            source_count.count AS source_count,
                            target_count.count AS target_count,
                            CASE
                                WHEN source_count.count = target_count.count THEN 'match'
                                ELSE 'mismatch'
                            END AS match_status
                        FROM source_count, target_count
                    """,
                    "blocking": True,
                }
            ),
        )

        # Create the state to get Athena query results
        get_row_counts = tasks.AthenaGetQueryResults(
            self,
            "Get row counts and match status",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                "source_count": "{% $states.result.ResultSet.Rows[1].Data[0].VarCharValue %}",
                "target_count": "{% $states.result.ResultSet.Rows[1].Data[1].VarCharValue %}",
                "match_status": "{% $states.result.ResultSet.Rows[1].Data[2].VarCharValue %}",
            },
        )

        # Create the state to check if row counts match
        check_row_counts = (
            sfn.Choice(self, "Check if row counts match")
            .when(
                sfn.Condition.string_equals(
                    "$match_status",
                    "match",
                ),
                sfn.Succeed(
                    self,
                    "Row counts match",
                    comment="Source and target tables have the same number of rows",
                ),
            )
            .otherwise(
                sfn.Fail(
                    self,
                    "Row counts do not match",
                    cause="Source and target tables have different row counts",
                    error="RowCountMismatch",
                )
            )
        )

        # Chain the states together
        count_rows_in_both_tables.next(get_row_counts).next(check_row_counts)

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = count_rows_in_both_tables
