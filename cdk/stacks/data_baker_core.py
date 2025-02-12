"""
A stack that creates core elements of the data baker stack.

These can then be exported and used by other stacks.

For example, the Athena Database and Workgroups
"""

from typing import List

import aws_cdk.aws_glue_alpha as glue
from aws_cdk import CfnOutput
from aws_cdk import aws_athena as athena
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
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=self.buckets_by_name[
                        data_baker_results_bucket.bucket_name
                    ].s3_url_for_object(key="dc-data-baker-athena-results")
                ),
            ),
        )
        self.glue_database = self.make_database()

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
