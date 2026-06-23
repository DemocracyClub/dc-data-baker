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


class DeleteStaleOutcodesConstruct(Construct):
    """
    Removes orphaned <outcode>.parquet files from an outcode-grouped product.

    The 'by outcode products are rebuilt by overwriting one file per outcode.
    This means that when we're rebuilding them we don't have to delete them all first.
    However it is possible that there will be outcodes in one addressbase generation,
    that don't appear in the subsequent one. In this case the outcode file with
    address(es) from the previous generation will linger on s3.
    They won't be updated because they're not in the source data.
    So deleting them makes sense.

    Workflow:
    1. Run an Athena query for outcodes in the target but not the source.
    2. Get the query results.
    3. Drop the header row.
    4. Map over each stale outcode and delete its <outcode>.parquet file.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    construct_id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        Executes Athena queries.
    delete_objects_lambda : lambda_.IFunction
        Deletes S3 objects by key (uses empty_bucket_by_prefix).
    source_table_name : str
        The 'outcode' list is derived from the 'postcode' column.
    target_table_name : str
        The outcode-grouped parquet table to reconcile. Needs an 'outcode' column.
    dest_bucket_name : str
        Bucket holding the <outcode>.parquet files.
    dest_path : str
        Prefix such that a file lives at {dest_path}/{outcode}.parquet.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        delete_objects_lambda: lambda_.IFunction,
        source_table_name: str,
        target_table_name: str,
        dest_bucket_name: str,
        dest_path: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        find_stale_outcodes = tasks.LambdaInvoke(
            self,
            f"{construct_id}: Find stale outcodes",
            lambda_function=athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "source_table_name": source_table_name,
                        "target_table_name": target_table_name,
                    },
                    "QueryString": """
                        SELECT DISTINCT outcode
                        FROM {target_table_name}
                        WHERE outcode NOT IN (
                            SELECT DISTINCT split_part(postcode, ' ', 1)
                            FROM {source_table_name}
                        )
                    """,
                    "blocking": True,
                }
            ),
        )

        get_stale_outcodes = tasks.AthenaGetQueryResults(
            self,
            f"{construct_id}: Get stale outcodes",
            query_language=sfn.QueryLanguage.JSONATA,
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
        )

        # Drop the Athena header row so the Map iterates only over data rows.
        drop_header = sfn.Pass(
            self,
            f"{construct_id}: Drop header from stale outcodes",
            parameters={
                "outcodes": sfn.JsonPath.string_at("$.ResultSet.Rows[1:]"),
            },
        )

        # Each item is an Athena row: {"Data": [{"VarCharValue": "HS9"}]}.
        # Delete exactly {dest_path}/{outcode}.parquet for that outcode.
        delete_outcode_file = tasks.LambdaInvoke(
            self,
            f"{construct_id}: Delete stale outcode file",
            lambda_function=delete_objects_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": dest_bucket_name,
                    "prefix": sfn.JsonPath.format(
                        dest_path + "/{}.parquet",
                        sfn.JsonPath.string_at("$.Data[0].VarCharValue"),
                    ),
                }
            ),
        )

        delete_stale_outcodes = sfn.Map(
            self,
            f"{construct_id}: Delete each stale outcode file",
            items_path="$.outcodes",
            max_concurrency=5,
        )
        delete_stale_outcodes.item_processor(
            delete_outcode_file, mode=sfn.ProcessorMode.INLINE
        )

        self.entry_point = (
            sfn.Chain.start(find_stale_outcodes)
            .next(get_stale_outcodes)
            .next(drop_header)
            .next(delete_stale_outcodes)
        )
