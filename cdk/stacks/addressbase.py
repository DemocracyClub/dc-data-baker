"""
A stack that creates and populates a Glue table
containing AddressBase, partitioned as Parquet files,
partitioned by the first letter of the postcode.


"""

from typing import List

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

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return [addressbase_cleaned_raw, addressbase_partitioned]

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return [pollingstations_private_data, data_baker_results_bucket]
