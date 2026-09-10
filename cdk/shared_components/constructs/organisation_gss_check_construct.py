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


class OrganisationGssCheckConstruct(Construct):
    """
    A CDK construct that checks each organisation has one distinct GSS value.

    The construct runs an Athena query for organisations with multiple GSS
    values, gets the results, and fails if any violating organisations exist.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    construct_id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        The Lambda function that will execute Athena queries
    table_name : str
        The table name to check. Must have 'organisation_name' and
        'organisation_gss' columns.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        table_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        query_multiple_gss_organisations = tasks.LambdaInvoke(
            self,
            f"{construct_id}: Find orgs with > 1 GSS values",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {"table_name": table_name},
                    "QueryString": """
                        SELECT
                            organisation_name,
                            count(DISTINCT organisation_gss) AS organisation_gss_count
                        FROM {table_name}
                        GROUP BY organisation_name
                        HAVING count(DISTINCT organisation_gss) > 1
                    """,
                    "blocking": True,
                }
            ),
        )

        get_query_results = tasks.AthenaGetQueryResults(
            self,
            f"{construct_id}: Get multiple GSS value results",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
        )

        count_results = sfn.Pass(
            self,
            f"{construct_id}: Count orgs with > 1 GSS values",
            parameters={
                "orgs_with_multiple_gss_length": sfn.JsonPath.string_at(
                    "States.ArrayLength($.ResultSet.Rows)"
                ),
            },
        )
        # We expect 1 header row in the query results
        check_results_count = (
            sfn.Choice(
                self,
                f"{construct_id}: Check org GSS values",
            )
            .when(
                sfn.Condition.number_equals(
                    "$.orgs_with_multiple_gss_length", 1
                ),
                sfn.Succeed(
                    self,
                    f"{construct_id}: Orgs have one GSS value",
                ),
            )
            .otherwise(
                sfn.Fail(
                    self,
                    f"{construct_id}: Orgs have multiple GSS values",
                )
            )
        )

        query_multiple_gss_organisations.next(get_query_results).next(
            count_results
        ).next(check_results_count)

        self.entry_point = query_multiple_gss_organisations
