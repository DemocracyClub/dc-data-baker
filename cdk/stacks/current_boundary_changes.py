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

        delete_old_current_boundary_changes_task = (
            self.make_delete_old_current_boundary_changes_task()
        )

        create_current_boundary_changes_csv_task = (
            self.make_current_boundary_changes_csv_task()
        )

        make_current_boundary_changes_partitions = self.make_partitions_task(
            current_boundary_changes
        )


        main_tasks = (
            delete_old_current_boundary_changes_task.next(
                create_current_boundary_changes_csv_task
            )
            .next(make_current_boundary_changes_partitions)
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
            addresses_to_boundary_change,
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
        )

    def make_partitions_task(self, table) -> tasks.LambdaInvoke:
        return tasks.LambdaInvoke(
            self,
            f"Make partitions for {table.table_name}",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {"table_name": table.table_name},
                    "QueryString": "MSCK REPAIR TABLE `$table_name`;",
                    "blocking": True,
                }
            ),
        )

            self,
        )

        )

