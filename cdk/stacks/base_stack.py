import abc
import hashlib
from pathlib import Path
from string import Template
from typing import List

import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_s3 as s3
import jsii
from aws_cdk import Fn, Stack
from aws_cdk import aws_athena as athena
from constructs import Construct
from shared_components.models import BaseQuery, GlueTable, S3Bucket


class CDKABCMetaClass(jsii.JSIIMeta, abc.ABCMeta):
    """
    Required to allow using @abc.abstractmethod on a subclass of
    Stack. We want to do this in order to require some methods / properties
    on stacks

    Used by DataBakerStack below.
    """

    pass


class DataBakerStack(abc.ABC, Stack, metaclass=CDKABCMetaClass):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.dc_environment = self.node.try_get_context("dc-environment")
        self.context = {"dc_environment": self.dc_environment}

        self.collect_buckets()
        self.make_tables()

    @staticmethod
    @abc.abstractmethod
    def glue_tables() -> List[GlueTable]: ...

    @staticmethod
    @abc.abstractmethod
    def s3_buckets() -> List[S3Bucket]: ...

    def get_query_text(query):
        with open(f"../queries/{query}", "r") as f:
            return f.read()

    def make_tables(self):
        self.tables_by_name = {}
        database = glue.Database.from_database_arn(
            self,
            "DataBakerGlueDatabaseName",
            database_arn=Fn.import_value("DataBakerGlueDatabaseArn"),
        )
        for table in self.glue_tables():
            columns = []
            for column_name, column_type in table.columns.items():
                columns.append(
                    glue.Column(name=column_name, type=column_type, comment="")
                )

            self.tables_by_name[table.table_name] = glue.S3Table(
                self,
                table.table_name,
                table_name=table.table_name,
                description=table.description,
                bucket=self.buckets_by_name[table.bucket.bucket_name],
                s3_prefix=table.s3_prefix.format(**self.context),
                database=database,
                columns=columns,
                data_format=table.data_format,
                partition_keys=table.partition_keys,
            )

            if table.populated_with:
                self.make_named_query(table, table.populated_with)

    def make_named_query(self, table: GlueTable, query: BaseQuery):
        file_path = Path(__file__).parent.parent / "queries" / query.name
        assert file_path.exists()
        query_context = self.context.copy()
        query_context["table_full_s3_path"] = (
            f"s3://{table.bucket.bucket_name}/{table.s3_prefix.format(**self.context)}"
        )
        query_context.update(query.context)
        with file_path.open() as f:
            query_str = Template(f.read()).substitute(**query_context)

        # Use a hash to force the resource to be updated
        query_hash = hashlib.md5(query_str.encode("utf-8")).hexdigest()
        return athena.CfnNamedQuery(
            self,
            f"{query.name}-id",
            database=table.database.database_name,
            query_string=query_str,
            name=query.name,
            work_group=Fn.import_value("DataBakerWorkgroupName"),
            description=f"Query version: {query_hash}",
        )

    def collect_buckets(self):
        self.buckets_by_name = {}

        for bucket in self.s3_buckets():
            self.buckets_by_name[bucket.bucket_name] = (
                s3.Bucket.from_bucket_name(
                    self,
                    bucket.bucket_name,
                    bucket.bucket_name,
                )
            )
