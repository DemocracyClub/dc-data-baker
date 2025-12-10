"""
A stack that creates and populates a Glue table
containing AddressBase, partitioned as Parquet files,
partitioned by the first letter of the postcode.


"""

from typing import List

from aws_cdk import (
    CfnOutput,
    Duration,
    Fn,
    aws_lambda,
)
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.buckets import (
    pollingstations_private_data,
)
from shared_components.constructs.addressbase_source_check_construct import (
    AddressBaseSourceCheckConstruct,
)
from shared_components.constructs.make_partitions_construct import (
    MakePartitionsConstruct,
)
from shared_components.databases import dc_data_baker
from shared_components.models import GlueTable, S3Bucket
from shared_components.tables import (
    addressbase_cleaned_raw,
    addressbase_partitioned,
)
from stacks.base_stack import DataBakerStack


class AddressBaseStack(DataBakerStack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.athena_query_lambda_arn = Fn.import_value(
            "RunAthenaQueryArnOutput"
        )
        self.athena_query_lambda = aws_lambda.Function.from_function_arn(
            self, "RunAthenaQuery", self.athena_query_lambda_arn
        )
        self.empty_bucket_by_prefix = Fn.import_value(
            "EmptyS3BucketByPrefixArnOutput"
        )
        self.empty_bucket_by_prefix_lambda = (
            aws_lambda.Function.from_function_arn(
                self, "EmptyS3BucketByPrefix", self.empty_bucket_by_prefix
            )
        )
        self.get_glue_table_location_arn = Fn.import_value(
            "GetGlueTableLocationArnOutput"
        )
        self.get_glue_table_location_lambda = (
            aws_lambda.Function.from_function_arn(
                self, "GetGlueTableLocation", self.get_glue_table_location_arn
            )
        )
        context = addressbase_partitioned.populated_with.context.copy()
        addressbase_source = "addressbase_source"
        get_addressbase_cleaned_raw_glue_table_location = tasks.LambdaInvoke(
            self,
            "Get addressbase cleaned raw glue table location",
            lambda_function=self.get_glue_table_location_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "database": dc_data_baker.database_name,
                    "table": addressbase_cleaned_raw.table_name,
                }
            ),
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                addressbase_source: "{% $states.result.Payload.addressbase_cleaned_raw_location %}"
            },
        )
        delete_old_objects = tasks.LambdaInvoke(
            self,
            "Remove old data from S3",
            lambda_function=self.empty_bucket_by_prefix_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket": addressbase_partitioned.bucket.bucket_name,
                    "prefix": addressbase_partitioned.s3_prefix.format(
                        **self.context
                    ),
                }
            ),
        )
        context[addressbase_source] = "{% $addressbase_source %}"
        partition = tasks.LambdaInvoke(
            self,
            "Partition AddressBase Cleaned",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": context,
                    "QueryName": addressbase_partitioned.populated_with.name,
                    "blocking": True,
                }
            ),
            query_language=sfn.QueryLanguage.JSONATA,
        )

        make_partitions = MakePartitionsConstruct(
            self,
            f"Make partitions for {addressbase_partitioned.table_name}",
            athena_query_lambda=self.athena_query_lambda,
            target_table_name=addressbase_partitioned.table_name,
        ).entry_point

        addressbase_check = AddressBaseSourceCheckConstruct(
            self,
            "AddressBaseSourceCheck",
            athena_query_lambda=self.athena_query_lambda,
            table_name=addressbase_partitioned.table_name,
        )

        self.state_definition = (
            sfn.Chain.start(get_addressbase_cleaned_raw_glue_table_location)
            .next(delete_old_objects)
            .next(partition)
            .next(make_partitions)
            .next(addressbase_check.entry_point)
        )

        self.step_function = sfn.StateMachine(
            self,
            "MakeAddressBasePartitioned",
            state_machine_name="MakeAddressBasePartitioned",
            definition=self.state_definition,
            timeout=Duration.minutes(10),
        )

        CfnOutput(
            self,
            "MakeAddressBasePartitionedArnOutput",
            value=self.step_function.state_machine_arn,
            export_name="MakeAddressBasePartitionedArn",
        )

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [addressbase_cleaned_raw, addressbase_partitioned]

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [pollingstations_private_data]
