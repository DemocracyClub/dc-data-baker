"""
A stack that creates and populates a Glue table
containing AddressBase, partitioned as Parquet files,
partitioned by the first letter of the postcode.


"""

from typing import List

from aws_cdk import (
    Duration,
    Fn,
    aws_lambda,
)
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.buckets import (
    data_baker_results_bucket,
    pollingstations_private_data,
)
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
        context = addressbase_partitioned.populated_with.context.copy()
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
        )

        make_partitions = tasks.LambdaInvoke(
            self,
            "Make partitions",
            lambda_function=self.athena_query_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "context": {
                        "table_name": addressbase_partitioned.table_name
                    },
                    "QueryString": "MSCK REPAIR TABLE `$table_name`;",
                    "blocking": True,
                }
            ),
        )

        definition = partition.next(make_partitions)

        sfn.StateMachine(
            self,
            "MakeAddressBasePartitioned",
            state_machine_name="MakeAddressBasePartitioned",
            definition=definition,
            timeout=Duration.minutes(10),
        )

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [addressbase_cleaned_raw, addressbase_partitioned]

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [pollingstations_private_data, data_baker_results_bucket]
