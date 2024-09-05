import aws_cdk.aws_glue_alpha as glue
from layers.buckets import pollingstations_private_data
from layers.databases import dc_data_baker
from layers.models import BaseQuery, BaseTable

addressbase_cleaned_raw = BaseTable(
    table_name="addressbase_cleaned_raw",
    description="Addressbase table as produced for loading into WDIV",
    bucket=pollingstations_private_data,
    s3_prefix="addressbase/current/addressbase_cleaned_raw/",
    database=dc_data_baker,
    data_format=glue.DataFormat.CSV,
    columns={
        "uprn": glue.Schema.STRING,
        "address": glue.Schema.STRING,
        "postcode": glue.Schema.STRING,
        "location": glue.Schema.STRING,
        "address_type": glue.Schema.STRING,
    },
)

addressbase_partitioned = BaseTable(
    table_name="addressbase_partitioned",
    description="Addressbase table partitioned by first letter of postcode. With latitude and longitude columns",
    s3_prefix="addressbase/{dc_environment}/addressbase_partitioned/",
    bucket=pollingstations_private_data,
    database=dc_data_baker,
    data_format=glue.DataFormat.PARQUET,
    columns={
        "outcode": glue.Schema.STRING,
        "uprn": glue.Schema.STRING,
        "address": glue.Schema.STRING,
        "postcode": glue.Schema.STRING,
        "longitude": glue.Schema.DOUBLE,
        "latitude": glue.Schema.DOUBLE,
    },
    partition_keys=[
        glue.Column(
            name="first_letter",
            type=glue.Schema.STRING,
        )
    ],
    depends_on=[addressbase_cleaned_raw],
    populated_with=BaseQuery(
        name="partition-addressbase-cleaned.sql",
        context={"from_table": addressbase_cleaned_raw.table_name},
    ),
)

TABLES = [addressbase_cleaned_raw, addressbase_partitioned]
