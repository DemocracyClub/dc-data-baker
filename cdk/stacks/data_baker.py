from pathlib import Path

import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_lambda as aws_lambda
import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
import aws_cdk.aws_s3 as s3
from aws_cdk import Duration, Stack
from aws_cdk import aws_athena as athena
from constructs import Construct


def get_query_text(query):
    query_path = Path(__file__).parents[1] / "queries" / f"{query}.sql"
    with query_path.open("r") as f:
        return f.read()


class S3Paths:
    def __init__(self, dc_environment: str):
        self.dc_environment = dc_environment

        assert self.dc_environment in (
            valid_environments := (
                "development",
                "staging",
                "prod",
            )
        ), f"context `dc-environment` must be one of {valid_environments}"

        if self.dc_environment in (
            "development",
            "staging",
        ):
            self.stage = "testing"
        else:
            self.stage = "current"

    @property
    def addressbase_bucket(self) -> str:
        return "pollingstations.private.data"

    @property
    def ballots_bucket(self) -> str:
        return "ee.data-cache.production"

    @property
    def addressbase_cleaned_raw_prefix(self) -> str:
        return f"addressbase/{self.stage}/addressbase_cleaned/"

    @property
    def addressbase_partitioned_prefix(self) -> str:
        return f"addressbase/{self.stage}/addressbase_partitioned/"

    @property
    def uprn_to_ballots_first_letter(self) -> str:
        return f"addressbase/{self.stage}/uprn-to-ballots-first-letter/"

    @property
    def current_ballots_prefix(self) -> str:
        return "ballots-with-wkt/"


class DataBakerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get DC environment
        self.dc_environment = self.node.try_get_context("dc-environment")

        # Helper for s3 paths
        self.s3_paths = S3Paths(self.dc_environment)

        # Glue database
        self.dc_data_baker_db = glue.Database(
            self,
            "dc-data-baker-db",
            database_name="dc_data_baker",
            description="Data base for tables defined by dc data baker",
        )

        # Buckets
        self.addressbase_bucket = s3.Bucket.from_bucket_name(
            self,
            "addressbase-source-addressbase_bucket",
            self.s3_paths.addressbase_bucket,
        )
        self.ballots_bucket = s3.Bucket.from_bucket_name(
            self, "ballots-source-bucket", self.s3_paths.ballots_bucket
        )

        # Athena workgroup
        self.workgroup = self.make_athena_workgroup()

        # Tables
        self.addressbase_cleaned_raw_table = (
            self.make_addressbase_cleaned_raw_table()
        )
        self.addressbase_partitioned_table = (
            self.make_addressbase_partitioned_table()
        )
        self.current_ballots_table = self.make_current_ballots_table()

        # Queries
        self.define_partition_addressbase_cleaned_query(
            self.addressbase_cleaned_raw_table.table_name,
            self.addressbase_bucket.s3_url_for_object(
                key=self.s3_paths.addressbase_partitioned_prefix
            ),
        )
        self.define_addressbase_partitioned_msck_repair_query()
        self.define_uprn_to_ballots_first_letter_query(
            self.current_ballots_table.table_name,
            self.addressbase_bucket.s3_url_for_object(
                key=self.s3_paths.uprn_to_ballots_first_letter
            ),
        )

        # Lambdas
        self.make_partition_addressbase_lambda()

    def make_partition_addressbase_lambda(
        self,
    ) -> aws_lambda_python.PythonFunction:
        return aws_lambda_python.PythonFunction(
            self,
            "invoke-partition-addressbase-cleaned-query",
            function_name="partition-addressbase-by-first-letter",
            entry="cdk/lambdas/partition-addressbase-by-first-letter",
            index="handler.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            timeout=Duration.minutes(3),
        )

    def make_addressbase_cleaned_raw_table(self) -> glue.S3Table:
        return glue.S3Table(
            self,
            "addressbase-cleaned-raw-table-id",
            table_name="addressbase_cleaned_raw",
            bucket=self.addressbase_bucket,
            s3_prefix=self.s3_paths.addressbase_cleaned_raw_prefix,
            database=self.dc_data_baker_db,
            columns=[
                glue.Column(name="uprn", type=glue.Schema.STRING, comment=""),
                glue.Column(
                    name="address", type=glue.Schema.STRING, comment=""
                ),
                glue.Column(
                    name="postcode",
                    type=glue.Schema.STRING,
                    comment="With Space",
                ),
                glue.Column(
                    name="location",
                    type=glue.Schema.STRING,
                    comment="EWKT formatted, SRID=4326",
                ),
                glue.Column(
                    name="address_type",
                    type=glue.Schema.STRING,
                    comment="OS coding for the type of address",
                ),
            ],
            data_format=glue.DataFormat.CSV,
            description="Addressbase table as produced for loading into WDIV",
        )

    def make_current_ballots_table(self) -> glue.S3Table:
        return glue.S3Table(
            self,
            "current-ballots-table-id",
            table_name="current_ballots",
            bucket=self.ballots_bucket,
            s3_prefix=self.s3_paths.current_ballots_prefix,
            database=self.dc_data_baker_db,
            columns=[
                glue.Column(
                    name="election_id", type=glue.Schema.STRING, comment=""
                ),
                glue.Column(
                    name="division_id", type=glue.Schema.STRING, comment=""
                ),
                glue.Column(
                    name="geometry", type=glue.Schema.STRING, comment=""
                ),
                glue.Column(
                    name="source_table", type=glue.Schema.STRING, comment=""
                ),
            ],
            data_format=glue.DataFormat.CSV,
            description="Current ballot ids with wkt as csv",
        )

    def make_addressbase_partitioned_table(self) -> glue.S3Table:
        address_partitioned_table = glue.S3Table(
            self,
            "addressbase-partitioned-table-id",
            table_name="addressbase_partitioned",
            bucket=self.addressbase_bucket,
            s3_prefix=self.s3_paths.addressbase_partitioned_prefix,
            database=self.dc_data_baker_db,
            storage_parameters=[
                glue.StorageParameter.custom("serialization.format", "1")
            ],
            columns=[
                glue.Column(
                    name="outcode",
                    type=glue.Schema.STRING,
                ),
                glue.Column(
                    name="uprn",
                    type=glue.Schema.STRING,
                ),
                glue.Column(
                    name="address",
                    type=glue.Schema.STRING,
                ),
                glue.Column(
                    name="postcode",
                    type=glue.Schema.STRING,
                ),
                glue.Column(
                    name="longitude",
                    type=glue.Schema.DOUBLE,
                ),
                glue.Column(
                    name="latitude",
                    type=glue.Schema.DOUBLE,
                ),
            ],
            partition_keys=[
                glue.Column(
                    name="first_letter",
                    type=glue.Schema.STRING,
                )
            ],
            data_format=glue.DataFormat.PARQUET,
            description="Addressbase table partitioned by first letter of postcode. "
            "With latitude and longitude columns",
        )
        address_partitioned_table.add_partition_index(
            key_names=["first_letter"], index_name="first_letter"
        )
        return address_partitioned_table

    def partition_addressbase_cleaned_query_string(
        self, from_table: str, s3_target_dir: str
    ) -> str:
        base_query = get_query_text("partition-addressbase-cleaned")
        base_query = base_query.replace("$$from_table$$", from_table)
        return f"""
        UNLOAD ({base_query})
        TO '{s3_target_dir}'
        WITH(
            format = 'PARQUET',
            compression = 'SNAPPY',
            partitioned_by = ARRAY['first_letter']
        )
        """

    def uprn_to_ballots_first_letter_query_string(
        self, from_table: str, s3_target_dir: str
    ) -> str:
        base_query = get_query_text("uprn-to-ballots-first-letter")
        base_query = base_query.replace("$$from_table$$", from_table)
        return f"""
        UNLOAD ({base_query})
        TO '{s3_target_dir}'
        WITH(
            format = 'PARQUET',
            compression = 'SNAPPY',
            partitioned_by = ARRAY['first_letter']
        )
        """

    def define_partition_addressbase_cleaned_query(
        self, from_table: str, s3_target_dir: str
    ) -> athena.CfnNamedQuery:
        name = "partition-addressbase-cleaned"
        return athena.CfnNamedQuery(
            self,
            f"{name}-id",
            database=self.dc_data_baker_db.database_name,
            query_string=self.partition_addressbase_cleaned_query_string(
                from_table, s3_target_dir
            ),
            name=name,
            work_group=self.workgroup.name,
        )

    def define_addressbase_partitioned_msck_repair_query(
        self,
    ) -> athena.CfnNamedQuery:
        name = "addressbase-partitioned-msck-repair"
        return athena.CfnNamedQuery(
            self,
            f"{name}-id",
            database=self.dc_data_baker_db.database_name,
            query_string=f"MSCK REPAIR TABLE {self.addressbase_partitioned_table.table_name}",
            name=name,
            work_group=self.workgroup.name,
        )

    def define_uprn_to_ballots_first_letter_query(
        self, from_table: str, s3_target_dir: str
    ) -> athena.CfnNamedQuery:
        name = "uprn-to-ballots-first-letter"
        return athena.CfnNamedQuery(
            self,
            f"{name}-id",
            database=self.dc_data_baker_db.database_name,
            query_string=self.uprn_to_ballots_first_letter_query_string(
                from_table, s3_target_dir
            ),
            name=name,
            work_group=self.workgroup.name,
        )

    def make_athena_workgroup(self) -> athena.CfnWorkGroup:
        return athena.CfnWorkGroup(
            self,
            "dc-data-baker-workgroup-id",
            name="dc-data-baker",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=self.results_bucket().s3_url_for_object(
                        key="dc-data-baker-athena-results"
                    )
                ),
            ),
        )

    def results_bucket(self) -> s3.Bucket:
        return s3.Bucket(self, "dc-data-baker-results-addressbase-bucket")
