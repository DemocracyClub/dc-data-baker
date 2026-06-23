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


class UprnChecksumConstruct(Construct):
    """
    A CDK construct that checks two tables hold the same set of UPRNs.

    A single Athena query computes a checksum for both tables:

    - count(*)                          row count
    - count(DISTINCT uprn)              number of distinct UPRNs
    - to_hex(checksum(DISTINCT uprn))   an order-insensitive checksum of the
                                        distinct UPRN set

    checksum() is an aggregate which isn't affected by order. The row/UPRN counts
    are also calculated to help debug failed matches. And check for duplicates

    The construct creates a three-step workflow:
    1. Run the query against both tables.
    2. Get the query results.
    3. Compare the checksums and counts, continuing if they match, failing if not.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    construct_id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        The Lambda function that will execute Athena queries
    source_table_name : str
        The source table to compare against. Requires 'uprn' column.
    target_table_name : str
        The target table to compare against. Requires 'uprn' column.
    on_match : sfn.IChainable | None
        What to do on match. When omitted the construct
        terminates in a Succeed state. When supplied the match path continues
        into it instead, and the variables assigned by this construct are
        available to those downstream states.
    """

    # Variable names assigned by the "Get UPRN fingerprints" step. Exposed as
    # class attributes so callers can reference them in later states.
    SOURCE_ROW_COUNT = "source_row_count"
    TARGET_ROW_COUNT = "target_row_count"
    SOURCE_DISTINCT_COUNT = "source_distinct_count"
    TARGET_DISTINCT_COUNT = "target_distinct_count"
    SOURCE_UPRN_CHECKSUM = "source_uprn_checksum"
    TARGET_UPRN_CHECKSUM = "target_uprn_checksum"
    MATCH_STATUS = "match_status"

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        source_table_name: str,
        target_table_name: str,
        on_match: sfn.IChainable = None,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        compute_checksums = tasks.LambdaInvoke(
            self,
            "Compute UPRN checksums for source and target",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "source_table_name": source_table_name,
                        "target_table_name": target_table_name,
                    },
                    "QueryString": """
                        WITH source AS (
                            SELECT
                                count(*) AS row_count,
                                count(DISTINCT uprn) AS distinct_count,
                                to_hex(checksum(DISTINCT uprn)) AS uprn_checksum
                            FROM {source_table_name}
                        ),
                        target AS (
                            SELECT
                                count(*) AS row_count,
                                count(DISTINCT uprn) AS distinct_count,
                                to_hex(checksum(DISTINCT uprn)) AS uprn_checksum
                            FROM {target_table_name}
                        )
                        SELECT
                            source.row_count AS source_row_count,
                            target.row_count AS target_row_count,
                            source.distinct_count AS source_distinct_count,
                            target.distinct_count AS target_distinct_count,
                            source.uprn_checksum AS source_uprn_checksum,
                            target.uprn_checksum AS target_uprn_checksum,
                            CASE
                                WHEN source.distinct_count = target.distinct_count
                                 AND target.distinct_count = target.row_count
                                 AND source.uprn_checksum = target.uprn_checksum
                                THEN 'match'
                                ELSE 'mismatch'
                            END AS match_status
                        FROM source, target
                    """,
                    "blocking": True,
                }
            ),
        )

        get_checksums = tasks.AthenaGetQueryResults(
            self,
            "Get UPRN checksums and match status",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                self.SOURCE_ROW_COUNT: "{% $states.result.ResultSet.Rows[1].Data[0].VarCharValue %}",
                self.TARGET_ROW_COUNT: "{% $states.result.ResultSet.Rows[1].Data[1].VarCharValue %}",
                self.SOURCE_DISTINCT_COUNT: "{% $states.result.ResultSet.Rows[1].Data[2].VarCharValue %}",
                self.TARGET_DISTINCT_COUNT: "{% $states.result.ResultSet.Rows[1].Data[3].VarCharValue %}",
                self.SOURCE_UPRN_CHECKSUM: "{% $states.result.ResultSet.Rows[1].Data[4].VarCharValue %}",
                self.TARGET_UPRN_CHECKSUM: "{% $states.result.ResultSet.Rows[1].Data[5].VarCharValue %}",
                self.MATCH_STATUS: "{% $states.result.ResultSet.Rows[1].Data[6].VarCharValue %}",
            },
        )

        match_outcome = (
            on_match
            if on_match is not None
            else sfn.Succeed(
                self,
                "UPRN checksums match",
                comment="Source and target tables hold the same set of UPRNs",
            )
        )

        check_checksums = (
            sfn.Choice(self, "Check if UPRN checksums match")
            .when(
                sfn.Condition.string_equals(
                    f"${self.MATCH_STATUS}",
                    "match",
                ),
                match_outcome,
            )
            .otherwise(
                sfn.Fail(
                    self,
                    "UPRN checksums do not match",
                    cause="Source and target tables hold different sets of UPRNs",
                    error="UprnChecksumMismatch",
                )
            )
        )

        compute_checksums.next(get_checksums).next(check_checksums)

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = compute_checksums
