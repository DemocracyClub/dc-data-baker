from pathlib import Path
from string import Template

import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_s3 as s3
from aws_cdk import Stack
from aws_cdk import aws_athena as athena
from constructs import Construct
from layers.buckets import BUCKETS, data_baker_results_bucket
from layers.databases import DATABASES
from layers.models import BaseQuery, BaseTable
from layers.tables import TABLES


def get_query_text(query):
    with open(f"../queries/{query}", "r") as f:
        return f.read()


class DataBakerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dc_environment = self.node.try_get_context("dc-environment")

        self.context = {
            "dc_environment": self.dc_environment
        }

        self.make_databases()
        self.collect_buckets()

        # Athena workgroup
        self.workgroup = self.make_athena_workgroup()

        self.make_tables()

    def make_athena_workgroup(self) -> athena.CfnWorkGroup:
        return athena.CfnWorkGroup(
            self,
            "dc-data-baker-workgroup-id",
            name="dc-data-baker",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=self.buckets_by_name[data_baker_results_bucket.bucket_name].s3_url_for_object(
                        key="dc-data-baker-athena-results"
                    )
                ),
            ),
        )

    def make_databases(self):
        self.databases_by_name = {}
        for database in DATABASES:
            self.databases_by_name[database.database_name] = glue.Database(
                self,
                database.database_name,
                database_name=database.database_name,
                description="Data base for tables defined by dc data baker",
            )

    def collect_buckets(self):
        self.buckets_by_name = {}

        for bucket in BUCKETS:
            self.buckets_by_name[bucket.bucket_name] = s3.Bucket.from_bucket_name(
                self,
                bucket.bucket_name,
                bucket.bucket_name,
            )

    def make_tables(self):
        self.tables_by_name = {}
        for table in TABLES:
            columns = []
            for column_name, column_type in table.columns.items():
                columns.append(glue.Column(
                    name=column_name, type=column_type, comment=""
                ))

            self.tables_by_name[table.table_name] = glue.S3Table(
                self,
                table.table_name,
                table_name=table.table_name,
                description=table.description,
                bucket=self.buckets_by_name[table.bucket.bucket_name],
                s3_prefix=table.s3_prefix.format(**self.context),
                database=self.databases_by_name[table.database.database_name],
                columns=columns,
                data_format=table.data_format,
                partition_keys=table.partition_keys
            )

            if table.populated_with:
                self.make_named_query(table, table.populated_with)

    def make_named_query(self, table: BaseTable, query: BaseQuery):
        file_path = Path(__file__).parent.parent / "queries" / query.name
        assert file_path.exists()
        query_context = self.context.copy()
        query_context["table_full_s3_path"] = f"s3://{table.bucket.bucket_name}/{table.s3_prefix.format(**self.context)}"
        query_context.update(query.context)
        with file_path.open() as f:
            query_str = Template(f.read()).substitute(**query_context)

        return athena.CfnNamedQuery(
            self,
            f"{query.name}-id",
            database=table.database.database_name,
            query_string=query_str,
            name=query.name,
            work_group=self.workgroup.name,
        )
