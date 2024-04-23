import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_s3 as s3
from aws_cdk import Stack
from constructs import Construct


def get_query_text(query):
    with open(f"../queries/{query}", "r") as f:
        return f.read()


class DataBakerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dc_environment = self.node.try_get_context("dc-environment")

        self.dc_data_baker_db = glue.Database(
            self,
            "dc-data-baker-db",
            database_name="dc_data_baker",
            description="Data base for tables defined by dc data baker",
        )

        self.pollingstations_private_data_bucket = s3.Bucket.from_bucket_name(
            self,
            "pollingstations-private-data-bucket",
            "pollingstations.private.data",
        )

        self.addressbase_cleaned_raw_table()

    def addressbase_cleaned_raw_table(self):
        glue.S3Table(
            self,
            "addressbase-cleaned-raw-table-id",
            table_name="addressbase_cleaned_raw",
            bucket=self.pollingstations_private_data_bucket,
            s3_prefix="addressbase/current/addressbase_cleaned_raw/",
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
