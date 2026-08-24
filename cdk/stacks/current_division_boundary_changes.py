from typing import List

from aws_cdk import (
    CfnOutput,
    Fn,
    aws_lambda,
)
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.buckets import (
    data_baker_results_bucket,
    pollingstations_private_data,
)
from shared_components.constructs.addressbase_data_quality_check_construct import (
    AddressbaseDataQualityCheckConstruct,
)
from shared_components.constructs.make_partitions_construct import (
    MakePartitionsConstruct,
)
from shared_components.constructs.singleton_state_machine_construct import (
    SingletonStateMachineConstruct,
)
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    addressbase_cleaned_raw,
    addresses_to_division_boundary_change,
    current_division_boundary_changes,
    current_mappable_boundary_reviews_joined_to_addressbase,
)
from stacks.base_stack import DataBakerStack


class CurrentDivisionBoundaryChangesStack(DataBakerStack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.athena_query_lambda_arn = Fn.import_value(
            "RunAthenaQueryArnOutput"
        )
        self.athena_query_lambda = aws_lambda.Function.from_function_arn(
            self, "RunAthenaQuery", self.athena_query_lambda_arn
        )
        self.empty_bucket_by_prefix = Fn.import_value(
            "EmptyS3BucketByPrefixArnOutput"
        )
        self.empty_bucket_by_prefix_lambda = (
            aws_lambda.Function.from_function_arn(
                self, "EmptyS3BucketByPrefix", self.empty_bucket_by_prefix
            )
        )

        self.first_letter_to_outcode_parquet_lambda_arn = Fn.import_value(
            "FirstLetterToOutcodeParquetLambdaArnOutput"
        )

        self.first_letter_to_outcode_parquet_lambda = (
            aws_lambda.Function.from_function_arn(
                self,
                "FirstLetterToOutcodeParquet",
                self.first_letter_to_outcode_parquet_lambda_arn,
            )
        )
        boundary_review_pairs_map = self.make_boundary_review_pairs_map()

        make_addresses_to_division_boundary_change_partitions = (
            self.make_partitions_task(addresses_to_division_boundary_change)
        )

        delete_old_current_mappable_boundary_reviews_joined_to_addressbase_task = self.make_delete_old_current_mappable_boundary_reviews_joined_to_addressbase_task()

        make_current_mappable_boundary_reviews_joined_to_addressbase_task = self.make_current_mappable_boundary_reviews_joined_to_addressbase_task()

        make_current_mappable_boundary_reviews_joined_to_addressbase_partitions = self.make_partitions_task(
            current_mappable_boundary_reviews_joined_to_addressbase
        )

        first_letter_data_quality_checks = AddressbaseDataQualityCheckConstruct(
            self,
            "FirstLetterAddressbaseDataQualityChecks",
            athena_query_lambda=self.athena_query_lambda,
            source_table_name=addressbase_cleaned_raw.table_name,
            target_table_name=current_mappable_boundary_reviews_joined_to_addressbase.table_name,
        )

        main_tasks = (
            boundary_review_pairs_map.next(
                make_addresses_to_division_boundary_change_partitions
            )
            .next(
                delete_old_current_mappable_boundary_reviews_joined_to_addressbase_task
            )
            .next(
                make_current_mappable_boundary_reviews_joined_to_addressbase_task
            )
            .next(
                make_current_mappable_boundary_reviews_joined_to_addressbase_partitions
            )
            .next(first_letter_data_quality_checks.entry_point)
        )

        self.step_function = SingletonStateMachineConstruct(
            self,
            "MakeCurrentDivisionBoundaryChangesParquet",
            step_function_name="MakeCurrentDivisionBoundaryChangesParquet",
            main_tasks=main_tasks,
        ).entry_point

        CfnOutput(
            self,
            "MakeCurrentDivisionBoundaryChangesParquetArnOutput",
            value=self.step_function.state_machine_arn,
            export_name="MakeCurrentDivisionBoundaryChangesParquetArn",
        )

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [data_baker_results_bucket, pollingstations_private_data]

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [
            addresses_to_division_boundary_change,
            current_mappable_boundary_reviews_joined_to_addressbase,
        ]

    def make_partitions_task(self, table) -> tasks.LambdaInvoke:
        return MakePartitionsConstruct(
            self,
            f"MakePartitionsConstructFor{table.table_name}",
            athena_query_lambda=self.athena_query_lambda,
            target_table_name=table.table_name,
        ).entry_point

    def make_boundary_review_pairs_map(self) -> sfn.Chain:
        """
        Creates a workflow that:
        1. Deletes old data from addresses_to_division_boundary_change table
        2. Queries for unique boundary_review_id and division_type pairs
        3. Gets the query results
        4. Maps over each pair to run the addresses_to_division_boundary_change query
        """
        # Delete old data
        delete_old_addresses_to_division_boundary_change = tasks.LambdaInvoke(
            self,
            "Remove old addresses_to_division_boundary_change data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": addresses_to_division_boundary_change.bucket.bucket_name,
                    "prefix": addresses_to_division_boundary_change.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )

        # Query for unique pairs
        get_unique_pairs = tasks.LambdaInvoke(
            self,
            "start query to get unique boundary review / division type pairs",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "table_name": current_division_boundary_changes.table_name
                    },
                    "QueryString": """
                        SELECT DISTINCT
                            boundary_review_id,
                            division_type
                        FROM {table_name}
                        ORDER BY boundary_review_id, division_type
                    """,
                    "blocking": True,
                }
            ),
        )

        # Get the query results
        get_pairs_results = tasks.AthenaGetQueryResults(
            self,
            "Get unique BR/DT pairs results",
            query_language=sfn.QueryLanguage.JSONATA,
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
        )

        # Extract just the rows (skip header) to simplify downstream processing
        transform_results = sfn.Pass(
            self,
            "Drop header from unique BR/DT pairs",
            parameters={
                "pairs": sfn.JsonPath.string_at("$.ResultSet.Rows[1:]"),
            },
        )

        # Map task - process each pair
        # Each item will be a row from Athena: {"Data": [{"VarCharValue": "963"}, {"VarCharValue": "WAC"}]}
        # Pass it to the lambda that can run the athena query that populates 'addresses_to_division_boundary_change' table
        process_pair_task = tasks.LambdaInvoke(
            self,
            "Create Address to Boundary Review for Review/Division Type pair",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "boundary_review_id": sfn.JsonPath.string_at(
                            "$.Data[0].VarCharValue"
                        ),
                        "division_type": sfn.JsonPath.string_at(
                            "$.Data[1].VarCharValue"
                        ),
                    },
                    "QueryName": addresses_to_division_boundary_change.populated_with.name,
                    "blocking": True,
                }
            ),
        )

        map_state = sfn.Map(
            self,
            "Create Address to Boundary Review for each pair",
            items_path="$.pairs",
            max_concurrency=5,
        )
        map_state.item_processor(
            process_pair_task, mode=sfn.ProcessorMode.INLINE
        )

        # Chain the states together
        return (
            sfn.Chain.start(delete_old_addresses_to_division_boundary_change)
            .next(get_unique_pairs)
            .next(get_pairs_results)
            .next(transform_results)
            .next(map_state)
        )

    def make_delete_old_current_mappable_boundary_reviews_joined_to_addressbase_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Remove old current_mappable_boundary_reviews_joined_to_ab data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": current_mappable_boundary_reviews_joined_to_addressbase.bucket.bucket_name,
                    "prefix": current_mappable_boundary_reviews_joined_to_addressbase.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )

    def make_current_mappable_boundary_reviews_joined_to_addressbase_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Create current_mappable_boundary_reviews_joined_to_addressbase",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": current_mappable_boundary_reviews_joined_to_addressbase.populated_with.context.copy(),
                    "QueryName": current_mappable_boundary_reviews_joined_to_addressbase.populated_with.name,
                    "blocking": True,
                }
            ),
        )
