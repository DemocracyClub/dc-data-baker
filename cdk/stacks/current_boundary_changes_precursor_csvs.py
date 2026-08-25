"""
This stack creates the precursor CSV files for the current boundary changes
pipeline, including current division boundary changes and current pre-division
boundary reviews, and validates the generated data before downstream parquet jobs run.
The EE queries that create the CSVs are run in series so as not to overload EE.
"""

from typing import List

import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import (
    CfnOutput,
    Duration,
    Fn,
    aws_lambda,
)
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.buckets import (
    data_baker_results_bucket,
)
from shared_components.constructs.make_partitions_construct import (
    MakePartitionsConstruct,
)
from shared_components.constructs.singleton_state_machine_construct import (
    SingletonStateMachineConstruct,
)
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    current_division_boundary_changes,
    current_pre_division_boundary_reviews,
)
from stacks.base_stack import DataBakerStack


class CurrentBoundaryChangesPrecursorCSVsStack(DataBakerStack):
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

        delete_old_csvs_in_parallel = self.make_delete_old_csvs_in_parallel()

        create_current_division_boundary_changes_csv_task = (
            self.make_current_division_boundary_changes_csv_task()
        )

        make_current_division_boundary_changes_partitions = (
            self.make_partitions_task(current_division_boundary_changes)
        )

        current_division_boundary_changes_csv_quality_check = (
            self.make_current_division_boundary_changes_csv_quality_check()
        )

        create_current_pre_division_boundary_reviews_csv_task = (
            self.make_current_pre_division_boundary_reviews_csv_task()
        )

        make_current_pre_division_boundary_reviews_partitions_task = (
            self.make_partitions_task(current_pre_division_boundary_reviews)
        )

        main_tasks = (
            delete_old_csvs_in_parallel.next(
                create_current_division_boundary_changes_csv_task
            )
            .next(make_current_division_boundary_changes_partitions)
            .next(current_division_boundary_changes_csv_quality_check)
            .next(create_current_pre_division_boundary_reviews_csv_task)
            .next(make_current_pre_division_boundary_reviews_partitions_task)
        )

        self.step_function = SingletonStateMachineConstruct(
            self,
            "MakeCurrentBoundaryChangesPrecursorCSVsStack",
            step_function_name="CurrentBoundaryChangesPrecursorCSVsStack",
            main_tasks=main_tasks,
        ).entry_point

        CfnOutput(
            self,
            "MakeCurrentBoundaryChangesPrecursorCSVsOutput",
            value=self.step_function.state_machine_arn,
            export_name="MakeCurrentBoundaryChangesPrecursorCSVsArn",
        )

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [data_baker_results_bucket]

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [
            current_division_boundary_changes,
            current_pre_division_boundary_reviews,
        ]

    def make_partitions_task(self, table) -> tasks.LambdaInvoke:
        return MakePartitionsConstruct(
            self,
            f"MakePartitionsConstructFor{table.table_name}",
            athena_query_lambda=self.athena_query_lambda,
            target_table_name=table.table_name,
        ).entry_point

    def make_delete_old_csvs_in_parallel(self) -> sfn.Parallel:
        delete_old_csvs_in_parallel = sfn.Parallel(
            self,
            "Delete old CSVs in parallel",
            comment="Delete old CSVs in parallel",
        )

        old_csvs = [
            current_division_boundary_changes,
            current_pre_division_boundary_reviews,
        ]
        for table in old_csvs:
            delete_old_csv_task = self.make_delete_old_csv_task(table)
            delete_old_csvs_in_parallel.branch(delete_old_csv_task)

        return delete_old_csvs_in_parallel

    def make_delete_old_csv_task(self, table: GlueTable) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            f"Remove old data for {table.table_name} from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": table.bucket.bucket_name,
                    "prefix": table.s3_prefix.format(**self.context),
                }
            ),
        )

    def make_current_division_boundary_changes_csv_task(
        self,
    ) -> tasks.LambdaInvoke:
        create_current_division_boundary_changes_csv_function = aws_lambda_python.PythonFunction(
            self,
            "create_current_division_boundary_changes_csv",
            function_name="create_current_division_boundary_changes_csv",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/create_boundary_changes_csv",
            index="create_current_boundary_reviews_csv.py",
            timeout=Duration.seconds(900),
            memory_size=2048,
        )

        create_current_division_boundary_changes_csv_function.add_to_role_policy(
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
            lambda_function=create_current_division_boundary_changes_csv_function,
            payload=sfn.TaskInput.from_object(
                {
                    "s3_bucket": current_division_boundary_changes.bucket.bucket_name,
                    "s3_prefix": current_division_boundary_changes.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )

    def make_current_division_boundary_changes_csv_quality_check(
        self,
    ) -> sfn.Chain:
        division_ballots_query = tasks.LambdaInvoke(
            self,
            "Get divison ballots with more than one ballot",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "table_name": current_division_boundary_changes.table_name
                    },
                    "QueryString": "SELECT DISTINCT division_official_identifier, json_array_length(json_parse(division_related_ballots)) as division_ballot_count FROM {table_name} WHERE divisionset_generation = 'new' AND json_array_length(json_parse(division_related_ballots)) > 1;",
                    "blocking": True,
                }
            ),
        )

        # get results of above Athena query
        get_division_ballots_query_result = tasks.AthenaGetQueryResults(
            self,
            "Get divison ballots with more than one ballot results",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
        )
        # drop the header row
        remove_headers = sfn.Pass(
            self,
            "Drop header from division ballots query results",
            parameters={
                "divisions_and_counts": sfn.JsonPath.string_at(
                    "$.ResultSet.Rows[1:]"
                ),
            },
        )
        count_results = sfn.Pass(
            self,
            "Count division ballots query results rows",
            query_language=sfn.QueryLanguage.JSONATA,
            outputs={
                "divisions_and_counts_length": "{% $count($states.input.divisions_and_counts) %}",
            },
        )
        # check each division has exactly one ballot
        check_results_count = (
            sfn.Choice(self, "Check No divisions have more than one ballot")
            .when(
                sfn.Condition.number_equals("$.divisions_and_counts_length", 0),
                sfn.Pass(
                    self,
                    "No divisions have more than one ballot!",
                ),
            )
            .otherwise(
                sfn.Fail(self, "Some divisions have more than one ballot :(")
            )
        )

        return (
            division_ballots_query.next(get_division_ballots_query_result)
            .next(remove_headers)
            .next(count_results)
            .next(check_results_count.afterwards())
        )

    def make_current_pre_division_boundary_reviews_csv_task(
        self,
    ) -> tasks.LambdaInvoke:
        create_current_pre_division_boundary_reviews_csv_function = aws_lambda_python.PythonFunction(
            self,
            "create_current_pre_division_boundary_reviews_csv",
            function_name="create_current_pre_division_boundary_reviews_csv",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/create_pre_division_boundary_reviews_csv",
            index="create_current_pre_division_boundary_reviews_csv.py",
            timeout=Duration.seconds(900),
            memory_size=2048,
        )

        create_current_pre_division_boundary_reviews_csv_function.add_to_role_policy(
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
            "Make current pre-division boundary reviews CSV",
            lambda_function=create_current_pre_division_boundary_reviews_csv_function,
            payload=sfn.TaskInput.from_object(
                {
                    "s3_bucket": current_pre_division_boundary_reviews.bucket.bucket_name,
                    "s3_prefix": current_pre_division_boundary_reviews.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )
