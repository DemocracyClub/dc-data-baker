from typing import List

import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import Duration, Fn, aws_lambda
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.buckets import data_baker_results_bucket
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    current_boundary_changes,
    current_boundary_changes_joined_to_address_base,
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

        delete_old_current_boundary_changes_joined_to_address_base_task = self.make_delete_old_current_boundary_changes_joined_to_address_base_task()

        create_current_boundary_changes_csv_task = (
            self.make_current_boundary_changes_csv_task()
        )

        make_partitions = self.make_partitions_task()

        parallel_first_letter_task = self.make_parallel_first_letter_task()

        parallel_outcodes_task = self.make_parallel_outcodes_task()

        main_tasks = (
            delete_old_current_boundary_changes_joined_to_address_base_task.next(
                create_current_boundary_changes_csv_task
            )
            .next(parallel_first_letter_task)
            .next(make_partitions)
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
        return [data_baker_results_bucket]

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [
            current_boundary_changes,
            current_boundary_changes_joined_to_address_base,
        ]

    def make_delete_old_current_boundary_changes_joined_to_address_base_task(
        self,
    ) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Remove old data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": current_boundary_changes_joined_to_address_base.bucket.bucket_name,
                    "prefix": current_boundary_changes_joined_to_address_base.s3_prefix.format(
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
        )

    def make_parallel_first_letter_task(self) -> sfn.Parallel:
        # Fan-out step (for each letter A-Z)
        parallel_execution = sfn.Parallel(self, "Fan Out Letters")
        alphabet = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
        for letter in alphabet:
            context = current_boundary_changes_joined_to_address_base.populated_with.context.copy()
            context["first_letter"] = letter

            parallel_execution.branch(
                tasks.LambdaInvoke(
                    self,
                    f"Process {letter}",
                    lambda_function=self.athena_query_lambda,
                    payload=sfn.TaskInput.from_object(
                        {
                            "context": context,
                            "QueryName": current_boundary_changes_joined_to_address_base.populated_with.name,
                            "blocking": True,
                        }
                    ),
                )
            )
        return parallel_execution

    def make_partitions_task(self) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            "Make partitions",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "table_name": current_boundary_changes_joined_to_address_base.table_name
                    },
                    "QueryString": "MSCK REPAIR TABLE `$table_name`;",
                    "blocking": True,
                }
            ),
        )

    def make_parallel_outcodes_task(self) -> sfn.Parallel:
        to_outcode_parquet = self.make_to_outcode_parquet_task()
        parallel_outcodes = sfn.Parallel(
            self, "Make outcode parquet per first letter"
        )
        alphabet = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
        for letter in alphabet:
            context = current_boundary_changes_joined_to_address_base.populated_with.context.copy()
            context["first_letter"] = letter

            parallel_outcodes.branch(
                tasks.LambdaInvoke(
                    self,
                    f"Make outcode parquet for {letter}",
                    lambda_function=to_outcode_parquet,
                    payload=sfn.TaskInput.from_object(
                        {
                            "first_letter": letter,
                            "source_bucket_name": current_boundary_changes_joined_to_address_base.bucket.bucket_name,
                            "source_path": current_boundary_changes_joined_to_address_base.s3_prefix.format(
                                dc_environment=self.dc_environment
                            ),
                            "dest_bucket_name": data_baker_results_bucket.bucket_name,
                            "dest_path": "current_boundary_reviews_parquet",
                        }
                    ),
                )
            )
        return parallel_outcodes

    def make_to_outcode_parquet_task(self) -> tasks.LambdaInvoke:
        to_outcode_parquet = aws_lambda_python.PythonFunction(
            self,
            "first_letter_to_outcode_parquet_for_current_boundary_changes",
            function_name="first_letter_to_outcode_parquet_for_current_boundary_changes",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/first_letter_to_outcode_parquet_for_current_boundary_changes/",
            index="first_letter_to_outcode_parquet_for_current_boundary_changes.py",
            timeout=Duration.seconds(900),
            memory_size=2048,
        )

        to_outcode_parquet.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:*",
                    "s3:*",
                    "glue:*",
                ],
                resources=["*"],
            )
        )

        return to_outcode_parquet
