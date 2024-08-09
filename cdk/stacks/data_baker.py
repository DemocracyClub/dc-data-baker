import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_s3 as s3
from aws_cdk import Stack
from constructs import Construct
from layers.buckets import BUCKETS
from layers.databases import DATABASES
from layers.tables import TABLES


def get_query_text(query):
    with open(f"../queries/{query}", "r") as f:
        return f.read()


class DataBakerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dc_environment = self.node.try_get_context("dc-environment")

        self.make_databases()
        self.collect_buckets()
        self.make_tables()

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
            self.tables_by_name[table.table_name] = glue.S3Table(
                self,
                table.table_name,
                table_name=table.table_name,
                description=table.description,
                bucket=self.buckets_by_name[table.bucket.bucket_name],
                s3_prefix=table.s3_prefix,
                database=self.databases_by_name[table.database.database_name],
                columns=table.columns.as_glue_definition(),
                data_format=table.data_format,
            )
