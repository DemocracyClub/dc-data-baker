import aws_cdk.aws_glue_alpha as glue
from shared_components.buckets import (
    data_baker_results_bucket,
    ee_data_cache_production,
    pollingstations_private_data,
)
from shared_components.databases import dc_data_baker
from shared_components.models import BaseQuery, GlueTable

addressbase_cleaned_raw = GlueTable(
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

addressbase_partitioned = GlueTable(
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
        "addressbase_source": glue.Schema.STRING,
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

current_ballots = GlueTable(
    table_name="current_ballots",
    description="CSV in S3 generated by EE, contains each current ballot and the WKT of the geography",
    s3_prefix="ballots-with-wkt/",
    bucket=ee_data_cache_production,
    database=dc_data_baker,
    data_format=glue.DataFormat.CSV,
    columns={
        "election_id": glue.Schema.STRING,
        "division_id": glue.Schema.STRING,
        "geometry": glue.Schema.STRING,
        "source_table": glue.Schema.STRING,
    },
)

current_ballots_joined_to_address_base = GlueTable(
    table_name="current_ballots_joined_to_address_base",
    description="A list of current ballots per UPRN",
    s3_prefix="current_ballots_joined_to_address_base/",
    bucket=data_baker_results_bucket,
    database=dc_data_baker,
    data_format=glue.DataFormat.PARQUET,
    columns={
        "uprn": glue.Schema.STRING,
        "address": glue.Schema.STRING,
        "postcode": glue.Schema.STRING,
        "addressbase_source": glue.Schema.STRING,
        "ballot_ids": glue.Schema.array(
            input_string="string", is_primitive=True
        ),
        "first_letter": glue.Schema.STRING,
    },
    populated_with=BaseQuery(
        name="uprn-to-ballots-first-letter.sql",
        context={"from_table": current_ballots.table_name},
    ),
)
