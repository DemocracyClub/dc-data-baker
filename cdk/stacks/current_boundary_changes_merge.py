"""
This stack merges the current boundary review datasets with addressbase data,
creates the final per-outcode parquet outputs, clears stale outcode data,
and runs the source-quality checks for the final boundary change result set.
"""

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
from shared_components.constructs.addressbase_source_check_construct import (
    AddressBaseSourceCheckConstruct,
)
from shared_components.constructs.coordinated_singleton_state_machine_construct import (
    CoordinatedSingletonStateMachineConstruct,
)
from shared_components.constructs.delete_stale_outcodes_construct import (
    DeleteStaleOutcodesConstruct,
)
from shared_components.constructs.make_partitions_construct import (
    MakePartitionsConstruct,
)
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    addressbase_cleaned_raw,
    current_boundary_reviews_joined_to_addressbase,
    current_boundary_reviews_parquet,
)
from stacks.base_stack import DataBakerStack


class CurrentBoundaryChangesMergeStack(DataBakerStack):
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

        delete_old_current_boundary_reviews_joined_to_addressbase_task = self.make_delete_old_current_boundary_reviews_joined_to_addressbase_task()
        make_current_boundary_reviews_joined_to_addressbase_task = (
            self.make_current_boundary_reviews_joined_to_addressbase_task()
        )

        make_current_current_boundary_reviews_joined_to_addressbase_partitions_task = self.make_partitions_task(
            current_boundary_reviews_joined_to_addressbase
        )

        first_letter_data_quality_checks = AddressbaseDataQualityCheckConstruct(
            self,
            "FirstLetterAddressbaseDataQualityChecks",
            athena_query_lambda=self.athena_query_lambda,
            source_table_name=addressbase_cleaned_raw.table_name,
            target_table_name=current_boundary_reviews_joined_to_addressbase.table_name,
        )

        merge_quality_check = self.make_merge_quality_check()

        delete_stale_outcodes = DeleteStaleOutcodesConstruct(
            self,
            "DeleteStaleOutcodes",
            athena_query_lambda=self.athena_query_lambda,
            delete_objects_lambda=self.empty_bucket_by_prefix_lambda,
            source_table_name=current_boundary_reviews_joined_to_addressbase.table_name,
            target_table_name=current_boundary_reviews_parquet.table_name,
            dest_bucket_name=current_boundary_reviews_parquet.bucket.bucket_name,
            dest_path=current_boundary_reviews_parquet.s3_prefix.format(
                dc_environment=self.dc_environment
            ),
        )

        outcode_addressbase_source_check = AddressBaseSourceCheckConstruct(
            self,
            "OutcodeAddressbaseSourceCheck",
            athena_query_lambda=self.athena_query_lambda,
            table_name=current_boundary_reviews_parquet.table_name,
        )

        parallel_outcodes_task = self.make_parallel_outcodes_task()

        main_tasks = (
            delete_old_current_boundary_reviews_joined_to_addressbase_task.next(
                make_current_boundary_reviews_joined_to_addressbase_task
            )
            .next(
                make_current_current_boundary_reviews_joined_to_addressbase_partitions_task
            )
            .next(first_letter_data_quality_checks.entry_point)
            .next(merge_quality_check)
            .next(parallel_outcodes_task)
            .next(delete_stale_outcodes.entry_point)
            .next(outcode_addressbase_source_check.entry_point)
        )

        self.step_function = CoordinatedSingletonStateMachineConstruct(
            self,
            "MakeCurrentBoundaryChangesMergeStack",
            step_function_name="CurrentBoundaryChangesMergeStack",
            main_tasks=main_tasks,
        ).entry_point

        CfnOutput(
            self,
            "MakeCurrentBoundaryChangesMergeArnOutput",
            value=self.step_function.state_machine_arn,
            export_name="MakeCurrentBoundaryChangesMergeArn",
        )

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [
            data_baker_results_bucket,
            pollingstations_private_data,
        ]

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [
            current_boundary_reviews_joined_to_addressbase,
            current_boundary_reviews_parquet,
        ]

    def make_partitions_task(self, table) -> tasks.LambdaInvoke:
        return MakePartitionsConstruct(
            self,
            f"MakePartitionsConstructFor{table.table_name}",
            athena_query_lambda=self.athena_query_lambda,
            target_table_name=table.table_name,
        ).entry_point

    def make_run_state_machine_branch_from_arn(
        self, state_machine_arn: str, state_machine_id: str
    ) -> sfn.Chain:
        run_state_machine_task = self.make_run_state_machine_task_from_arn(
            state_machine_arn, state_machine_id
        )
        return sfn.Chain.start(run_state_machine_task)

    def make_run_state_machine_task_from_arn(
        self,
        state_machine_arn: str,
        state_machine_id: str,
    ) -> tasks.StepFunctionsStartExecution:
        state_machine = sfn.StateMachine.from_state_machine_arn(
            self,
            state_machine_id,
            state_machine_arn,
        )
        return tasks.StepFunctionsStartExecution(
            self,
            f"Run{state_machine_id}",
            state_machine=state_machine,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

    def make_delete_old_current_boundary_reviews_joined_to_addressbase_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Remove old current_boundary_reviews_joined_to_ab data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": current_boundary_reviews_joined_to_addressbase.bucket.bucket_name,
                    "prefix": current_boundary_reviews_joined_to_addressbase.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )

    def make_current_boundary_reviews_joined_to_addressbase_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Create current_boundary_reviews_joined_to_addressbase",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": current_boundary_reviews_joined_to_addressbase.populated_with.context.copy(),
                    "QueryName": current_boundary_reviews_joined_to_addressbase.populated_with.name,
                    "blocking": True,
                }
            ),
        )

    def make_merge_quality_check(self) -> sfn.Chain:
        # query for uprns with duplicated reviews
        merge_quality_check_query = tasks.LambdaInvoke(
            self,
            "Query for duplicated reviews",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": current_boundary_reviews_joined_to_addressbase.populated_with.context.copy(),
                    "QueryString": """
                    WITH review_ids AS (
                        SELECT
                            t.uprn,
                            json_extract_scalar(json_parse(u.review), '$.boundary_review_id') as review_id
                        FROM current_boundary_reviews_parquet AS t
                        CROSS JOIN UNNEST(t.boundary_reviews) AS u(review)
                        WHERE cardinality(t.boundary_reviews) > 1
                    )
                    SELECT DISTINCT review_id
                    FROM (
                        SELECT uprn, review_id
                        FROM review_ids
                        GROUP BY uprn, review_id
                        HAVING COUNT(*) > 1
                    )
                    """,
                    "blocking": True,
                }
            ),
        )
        # get the results
        get_merge_quality_check_results = tasks.AthenaGetQueryResults(
            self,
            "Get duplicated reviews query results",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
        )
        # count the number of duplicated review rows (includes 1 header row)
        get_row_count = sfn.Pass(
            self,
            "Count duplicated reviews query results",
            parameters={
                "duplicated_review_row_count": sfn.JsonPath.string_at(
                    "States.ArrayLength($.ResultSet.Rows)"
                ),
            },
        )

        # check results of query
        check_merge_quality = (
            sfn.Choice(self, "Check duplicated reviews query results")
            .when(
                sfn.Condition.number_equals("$.duplicated_review_row_count", 1),
                sfn.Pass(self, "No duplicated reviews!"),
            )
            .otherwise(sfn.Fail(self, "Some uprns have duplicated reviews!"))
        )
        return (
            merge_quality_check_query.next(get_merge_quality_check_results)
            .next(get_row_count)
            .next(check_merge_quality.afterwards())
        )

    def make_parallel_outcodes_task(self) -> sfn.Parallel:
        parallel_outcodes = sfn.Parallel(
            self, "Make outcode parquet per first letter"
        )
        alphabet = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
        for letter in alphabet:
            context = current_boundary_reviews_joined_to_addressbase.populated_with.context.copy()
            context["first_letter"] = letter

            parallel_outcodes.branch(
                tasks.LambdaInvoke(
                    self,
                    f"Make outcode parquet for {letter}",
                    lambda_function=self.first_letter_to_outcode_parquet_lambda,
                    payload=sfn.TaskInput.from_object(
                        {
                            "first_letter": letter,
                            "source_bucket_name": current_boundary_reviews_joined_to_addressbase.bucket.bucket_name,
                            "source_path": current_boundary_reviews_joined_to_addressbase.s3_prefix.format(
                                dc_environment=self.dc_environment
                            ),
                            "dest_bucket_name": current_boundary_reviews_parquet.bucket.bucket_name,
                            "dest_path": current_boundary_reviews_parquet.s3_prefix.format(
                                dc_environment=self.dc_environment
                            ),
                            "filter_column": "boundary_reviews",
                        }
                    ),
                )
            )
        return parallel_outcodes
