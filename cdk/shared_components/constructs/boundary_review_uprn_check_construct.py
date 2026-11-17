from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class BoundaryReviewUprnCheckConstruct(Construct):
    """Fail if a source boundary review has no joined UPRNs."""

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

        find_missing_reviews = tasks.LambdaInvoke(
            self,
            "Find boundary reviews without joined UPRNs",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "source_table": source_table_name,
                        "target_table": target_table_name,
                    },
                    "QueryString": """
                        SELECT DISTINCT source.boundary_review_id
                        FROM {source_table} source
                        LEFT JOIN (
                            SELECT DISTINCT boundary_review_id
                            FROM {target_table}
                            WHERE uprn IS NOT NULL
                        ) target
                            ON source.boundary_review_id = target.boundary_review_id
                        WHERE target.boundary_review_id IS NULL
                        ORDER BY source.boundary_review_id
                        LIMIT 1
                    """,
                    "blocking": True,
                }
            ),
        )
        get_missing_reviews = tasks.AthenaGetQueryResults(
            self,
            "Get boundary reviews without joined UPRNs",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
        )
        check_results = (
            sfn.Choice(self, "Check boundary reviews have joined UPRNs")
            .when(
                sfn.Condition.is_present(
                    "$.ResultSet.Rows[1].Data[0].VarCharValue"
                ),
                sfn.Fail(
                    self,
                    "Boundary review has no joined UPRNs",
                    error="BoundaryReviewWithoutUPRNs",
                    cause_path=sfn.JsonPath.format(
                        "Boundary review {} has no joined UPRNs",
                        sfn.JsonPath.string_at(
                            "$.ResultSet.Rows[1].Data[0].VarCharValue"
                        ),
                    ),
                ),
            )
            .when(
                sfn.Condition.and_(
                    sfn.Condition.is_present(
                        "$.ResultSet.Rows[0].Data[0].VarCharValue"
                    ),
                    sfn.Condition.not_(
                        sfn.Condition.is_present("$.ResultSet.Rows[1]")
                    ),
                ),
                sfn.Pass(self, "All boundary reviews have joined UPRNs"),
            )
            .otherwise(
                sfn.Fail(
                    self,
                    "Unexpected boundary review check results",
                    error="InvalidBoundaryReviewCheckResults",
                )
            )
        )
        self.entry_point = find_missing_reviews.next(get_missing_reviews).next(
            check_results.afterwards()
        )
