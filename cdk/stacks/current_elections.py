"""
A stack that creates and populates a S3 bucket with
a parquet file per outcode.

Each file contains a list of ballots per UK address (from AddressBase).

This is generated from the AddressBase stack geo-joined with a CSV of
current elections.

The list of current elections is generated by EE. An update to that
list will trigger a re-build of this data package.

"""

from typing import List

import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import (
    Duration,
    Fn,
    aws_lambda,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.buckets import (
    data_baker_results_bucket,
    ee_data_cache_production,
)
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    current_ballots,
    current_ballots_joined_to_address_base,
)
from stacks.base_stack import DataBakerStack


class CurrentElectionsStack(DataBakerStack):
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

        delete_old_current_ballots_joined_to_address_base = tasks.LambdaInvoke(
            self,
            "Remove old data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": current_ballots_joined_to_address_base.bucket.bucket_name,
                    "prefix": current_ballots_joined_to_address_base.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )

        create_current_elections_csv_function = aws_lambda_python.PythonFunction(
            self,
            "create_current_elections_csv",
            function_name="create_current_elections_csv",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/create_current_elections_csv",
            index="create_current_elections_csv.py",
            timeout=Duration.seconds(900),
            memory_size=2048,
        )

        create_current_elections_csv_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:*",
                    "s3:*",
                ],
                resources=["*"],
            )
        )

        make_current_csv = tasks.LambdaInvoke(
            self,
            "Make current elections CSV",
            lambda_function=create_current_elections_csv_function,
        )

        # Fan-out step (for each letter A-Z)
        parallel_execution = sfn.Parallel(self, "Fan Out Letters")
        alphabet = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
        for letter in alphabet:
            context = current_ballots_joined_to_address_base.populated_with.context.copy()
            context["first_letter"] = letter

            parallel_execution.branch(
                tasks.LambdaInvoke(
                    self,
                    f"Process {letter}",
                    lambda_function=self.athena_query_lambda,
                    payload=sfn.TaskInput.from_object(
                        {
                            "context": context,
                            "QueryName": current_ballots_joined_to_address_base.populated_with.name,
                            "blocking": True,
                        }
                    ),
                )
            )

        make_partitions = tasks.LambdaInvoke(
            self,
            "Make partitions",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "table_name": current_ballots_joined_to_address_base.table_name
                    },
                    "QueryString": "MSCK REPAIR TABLE `$table_name`;",
                    "blocking": True,
                }
            ),
        )

        to_outcode_parquet = aws_lambda_python.PythonFunction(
            self,
            "first_letter_to_outcode_parquet",
            function_name="first_letter_to_outcode_parquet",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/first_letter_to_outcode_parquet/",
            index="first_letter_to_outcode_parquet.py",
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

        # Fan-out step (for each letter A-Z)
        parallel_outcodes = sfn.Parallel(
            self, "Make outcode parquet per first letter"
        )
        alphabet = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
        for letter in alphabet:
            context = current_ballots_joined_to_address_base.populated_with.context.copy()
            context["first_letter"] = letter

            parallel_outcodes.branch(
                tasks.LambdaInvoke(
                    self,
                    f"Make outcode parquet for {letter}",
                    lambda_function=to_outcode_parquet,
                    payload=sfn.TaskInput.from_object(
                        {
                            "first_letter": letter,
                            "source_bucket_name": "dc-data-baker-results-bucket",
                            "source_path": "current_ballots_joined_to_address_base",
                            "dest_bucket_name": "dc-data-baker-results-bucket",
                            "dest_path": "current_elections_parquet",
                        }
                    ),
                )
            )

        self.state_definition = (
            delete_old_current_ballots_joined_to_address_base.next(
                make_current_csv
            )
            .next(parallel_execution)
            .next(make_partitions)
            .next(parallel_outcodes)
        )

        self.step_function = sfn.StateMachine(
            self,
            "MakeCurrentElectionsParquet",
            state_machine_name="MakeCurrentElectionsParquet",
            definition=self.state_definition,
            timeout=Duration.minutes(10),
        )

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [current_ballots, current_ballots_joined_to_address_base]

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [ee_data_cache_production, data_baker_results_bucket]
