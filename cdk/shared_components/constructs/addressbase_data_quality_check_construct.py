from aws_cdk import (
    aws_lambda as lambda_,
)
from aws_cdk import (
    aws_stepfunctions as sfn,
)
from constructs import Construct
from shared_components.constructs.addressbase_source_check_construct import (
    AddressBaseSourceCheckConstruct,
)
from shared_components.constructs.row_count_check_construct import (
    RowCountCheckConstruct,
)


class AddressbaseDataQualityCheckConstruct(Construct):
    """
    A CDK construct that runs data quality checks on addressbase-joined tables.

    This construct runs two checks in parallel:
    1. Addressbase source check - verifies exactly one distinct addressbase source
    2. Row count check - verifies the target table has the same row count as addressbase

    The workflow succeeds only if both checks pass. If either check fails, the
    entire workflow fails with details about which check(s) failed.

    This is useful for validating that addressbase join operations preserve data
    integrity and don't introduce multiple source versions.

    Parameters:
    -----------
    scope : Construct
        The parent construct
    id : str
        The construct ID
    athena_query_lambda : lambda_.IFunction
        The Lambda function that will execute Athena queries
    target_table_name : str
        The target table to validate (must have addressbase_source column)
    source_table_name : str
        The source addressbase table to compare row counts against
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        athena_query_lambda: lambda_.IFunction,
        target_table_name: str,
        source_table_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Create the addressbase source check
        addressbase_source_check = AddressBaseSourceCheckConstruct(
            self,
            "AddressbaseSourceCheck",
            athena_query_lambda=athena_query_lambda,
            table_name=target_table_name,
        )

        # Create the row count check
        row_count_check = RowCountCheckConstruct(
            self,
            "RowCountCheck",
            athena_query_lambda=athena_query_lambda,
            source_table_name=source_table_name,
            target_table_name=target_table_name,
        )

        # Create a parallel state to run both checks concurrently
        parallel_checks = sfn.Parallel(
            self,
            "Run data quality checks in parallel",
            comment="Validates addressbase source uniqueness and row count match",
        )

        # Add both check branches to the parallel state
        parallel_checks.branch(addressbase_source_check.entry_point)
        parallel_checks.branch(row_count_check.entry_point)

        # Expose the entry point as a property to connect to other state machines
        self.entry_point = parallel_checks
