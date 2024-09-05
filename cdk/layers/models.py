from dataclasses import dataclass

from aws_cdk import aws_glue_alpha as glue


@dataclass
class S3Bucket:
    bucket_name: str


@dataclass
class GlueDatabase:
    database_name: str


@dataclass
class BaseQuery:
    name: str
    context: dict


@dataclass
class BaseTable:
    """
    A table is a Python data class that represents an AWS Glue definition

    It is used as a way to define Glue tables and to represent
    how they relate to each other.
    """
    table_name: str
    description: str
    bucket: S3Bucket
    s3_prefix: str
    database: GlueDatabase
    data_format: type(glue.DataFormat)
    columns: dict[str: glue.Schema]
    partition_keys: list[glue.Column] = None
    depends_on: list = None
    populated_with: BaseQuery = None
