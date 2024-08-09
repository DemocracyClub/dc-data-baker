from dataclasses import dataclass

from aws_cdk import aws_glue_alpha as glue


@dataclass
class S3Bucket:
    bucket_name: str


@dataclass
class GlueDatabase:
    database_name: str


@dataclass
class BaseTableColumns:
    ...

    @classmethod
    def as_glue_definition(cls):
        columns = []
        for column_name, python_type in cls.__annotations__.items():
            if python_type == str:
                column_type = glue.Schema.STRING

            columns.append(glue.Column(
                name=column_name, type=column_type, comment=""
            ))
        return columns


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
    columns: BaseTableColumns
