from typing import List

import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import Duration, Fn, aws_lambda
from aws_cdk import (
    aws_iam as iam,
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
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    addressbase_partitioned,
    addresses_to_boundary_change,
    current_boundary_changes,
    current_boundary_reviews_joined_to_addressbase,
    current_boundary_reviews_parquet,
)
from stacks.base_stack import DataBakerStack


class CurrentBoundaryChangesStack(DataBakerStack):
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
        self.check_step_function_running = Fn.import_value(
            "CheckStepFunctionRunningArnOutput"
        )
        self.check_step_function_running_function = (
            aws_lambda.Function.from_function_arn(
                self,
                "CheckStepFunctionRunningArnOutput",
                self.check_step_function_running,
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

        delete_old_current_boundary_changes_task = (
            self.make_delete_old_current_boundary_changes_task()
        )

        create_current_boundary_changes_csv_task = (
            self.make_current_boundary_changes_csv_task()
        )

        make_current_boundary_changes_partitions = self.make_partitions_task(
            current_boundary_changes
        )

        boundary_review_pairs_map = self.make_boundary_review_pairs_map()

        make_addresses_to_boundary_change_partitions = (
            self.make_partitions_task(addresses_to_boundary_change)
        )

        delete_old_current_boundary_reviews_joined_to_addressbase_task = self.make_delete_old_current_boundary_reviews_joined_to_addressbase_task()

        make_current_boundary_reviews_joined_to_addressbase_task = (
            self.make_current_boundary_reviews_joined_to_addressbase_task()
        )

        make_current_boundary_reviews_joined_to_addressbase_partitions = (
            self.make_partitions_task(
                current_boundary_reviews_joined_to_addressbase
            )
        )

        data_quality_checks = AddressbaseDataQualityCheckConstruct(
            self,
            "AddressbaseDataQualityChecks",
            athena_query_lambda=self.athena_query_lambda,
            source_table_name=addressbase_partitioned.table_name,
            target_table_name=current_boundary_reviews_joined_to_addressbase.table_name,
        )

        parallel_outcodes_task = self.make_parallel_outcodes_task()

        main_tasks = (
            delete_old_current_boundary_changes_task.next(
                create_current_boundary_changes_csv_task
            )
            .next(make_current_boundary_changes_partitions)
            .next(boundary_review_pairs_map)
            .next(make_addresses_to_boundary_change_partitions)
            .next(
                delete_old_current_boundary_reviews_joined_to_addressbase_task
            )
            .next(make_current_boundary_reviews_joined_to_addressbase_task)
            .next(
                make_current_boundary_reviews_joined_to_addressbase_partitions
            )
            .next(data_quality_checks.entry_point)
            .next(parallel_outcodes_task)
        )

        self.step_function = sfn.StateMachine(
            self,
            "MakeCurrentBoundaryChangesParquet",
            state_machine_name="MakeCurrentBoundaryChangesParquet",
            definition=main_tasks,
            timeout=Duration.minutes(10),
        )

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [data_baker_results_bucket, pollingstations_private_data]

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [
            current_boundary_changes,
            addresses_to_boundary_change,
            current_boundary_reviews_joined_to_addressbase,
            current_boundary_reviews_parquet,
        ]

    def make_delete_old_current_boundary_changes_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Remove old data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": current_boundary_changes.bucket.bucket_name,
                    "prefix": current_boundary_changes.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )

    def make_current_boundary_changes_csv_task(self) -> tasks.LambdaInvoke:
        create_current_boundary_changes_csv_function = aws_lambda_python.PythonFunction(
            self,
            "create_current_boundary_changes_csv",
            function_name="create_current_boundary_changes_csv",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/create_boundary_changes_csv",
            index="create_current_boundary_reviews_csv.py",
            timeout=Duration.seconds(900),
            memory_size=2048,
        )

        create_current_boundary_changes_csv_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:*",
                    "s3:*",
                ],
                resources=["*"],
            )
        )

        return tasks.LambdaInvoke(
            self,
            "Make current boundary changes CSV",
            lambda_function=create_current_boundary_changes_csv_function,
            payload=sfn.TaskInput.from_object(
                {
                    "s3_bucket": current_boundary_changes.bucket.bucket_name,
                    "s3_prefix": current_boundary_changes.s3_prefix,
                }
            ),
        )

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
        1. Deletes old data from addresses_to_boundary_change table
        2. Queries for unique boundary_review_id and division_type pairs
        3. Gets the query results
        4. Maps over each pair to run the addresses_to_boundary_change query
        """
        # Delete old data
        delete_old_addresses_to_boundary_change = tasks.LambdaInvoke(
            self,
            "Remove old addresses_to_boundary_change data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": addresses_to_boundary_change.bucket.bucket_name,
                    "prefix": addresses_to_boundary_change.s3_prefix,
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
                        "table_name": current_boundary_changes.table_name
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
        # Pass it to the lambda that can run the athena query that populates 'addresses_to_boundary_change' table
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
                    "QueryName": addresses_to_boundary_change.populated_with.name,
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
            sfn.Chain.start(delete_old_addresses_to_boundary_change)
            .next(get_unique_pairs)
            .next(get_pairs_results)
            .next(transform_results)
            .next(map_state)
        )

    def make_delete_old_current_boundary_reviews_joined_to_addressbase_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Remove old current_boundary_reviews_joined_to_addressbase data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": current_boundary_reviews_joined_to_addressbase.bucket.bucket_name,
                    "prefix": current_boundary_reviews_joined_to_addressbase.s3_prefix,
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
