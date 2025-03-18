"""
A stack that creates core elements of the data baker stack.

These can then be exported and used by other stacks.

For example, the Athena Database and Workgroups
"""

from typing import List

import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import CfnOutput, Duration, aws_lambda
from aws_cdk import aws_athena as athena
from aws_cdk import (
    aws_iam as iam,
)
from constructs import Construct
from shared_components.buckets import (
    data_baker_results_bucket,
)
from shared_components.databases import DEFAULT_DATABASE
from shared_components.models import GlueTable, S3Bucket
from stacks.base_stack import DataBakerStack


class DataBakerCoreStack(DataBakerStack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.make_athena_workgroup()

    def make_athena_workgroup(self):
        self.athena_workgroup = athena.CfnWorkGroup(
            self,
            "dc-data-baker-workgroup-id",
            name="dc-data-baker",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=self.buckets_by_name[
                        data_baker_results_bucket.bucket_name
                    ].s3_url_for_object(key="dc-data-baker-athena-results")
                ),
            ),
        )
        self.glue_database = self.make_database()

        run_athena_query_lambda = aws_lambda_python.PythonFunction(
            self,
            "run_athena_query_lambda",
            function_name="run_athena_query_lambda",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/",
            index="run_athena_query_and_report_status.py",
            timeout=Duration.seconds(900),
        )

        run_athena_query_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:*",
                    "s3:*",
                    "glue:*",
                ],
                resources=["*"],
            )
        )

        empty_s3_bucket_by_prefix_lambda = aws_lambda_python.PythonFunction(
            self,
            "empty_s3_bucket_by_prefix",
            function_name="empty_s3_bucket_by_prefix",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/",
            index="empty_s3_bucket_by_prefix.py",
            timeout=Duration.seconds(900),
        )

        empty_s3_bucket_by_prefix_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:*",
                ],
                resources=[
                    "arn:aws:s3:::*",
                ],
            )
        )

        check_step_function_running_function = aws_lambda_python.PythonFunction(
            self,
            "check_step_function_running",
            function_name="check_step_function_running",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/",
            index="check_step_function_running.py",
            timeout=Duration.seconds(900),
        )

        check_step_function_running_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "states:*",
                ],
                resources=[
                    "*",
                ],
            )
        )

        get_glue_table_location_lambda = aws_lambda_python.PythonFunction(
            self,
            "get_glue_table_location",
            function_name="get_glue_table_location",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler",
            entry="cdk/shared_components/lambdas/get_glue_table_location",
            index="get_glue_table_location.py",
            timeout=Duration.seconds(300),
        )

        get_glue_table_location_lambda.add_to_role_policy(
            iam.PolicyStatement(actions=["glue:GetTable"], resources=["*"]),
        )

        CfnOutput(
            self,
            "WorkgroupNameOutput",
            value=self.athena_workgroup.name,
            export_name="DataBakerWorkgroupName",
        )

        CfnOutput(
            self,
            "GlueDatabaseArnOutput",
            value=self.glue_database.database_arn,
            export_name="DataBakerGlueDatabaseArn",
        )
        CfnOutput(
            self,
            "RunAthenaQueryArnOutput",
            value=run_athena_query_lambda.function_arn,
            export_name="RunAthenaQueryArnOutput",
        )
        CfnOutput(
            self,
            "EmptyS3BucketByPrefixArnOutput",
            value=empty_s3_bucket_by_prefix_lambda.function_arn,
            export_name="EmptyS3BucketByPrefixArnOutput",
        )
        CfnOutput(
            self,
            "CheckStepFunctionRunningArnOutput",
            value=check_step_function_running_function.function_arn,
            export_name="CheckStepFunctionRunningArnOutput",
        )
        CfnOutput(
            self,
            "GetGlueTableLocationArnOutput",
            value=get_glue_table_location_lambda.function_arn,
            export_name="GetGlueTableLocationArnOutput",
        )

    def make_database(self):
        return glue.Database(
            self,
            DEFAULT_DATABASE.database_name,
            database_name=DEFAULT_DATABASE.database_name,
            description="Database for tables defined by DC data baker",
        )

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return []

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [data_baker_results_bucket]
