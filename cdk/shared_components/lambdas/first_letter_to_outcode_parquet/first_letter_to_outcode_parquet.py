import logging
import os
import shutil
import urllib.parse
from pathlib import Path

import boto3
import polars
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),
    environment=os.environ.get("DC_ENVIRONMENT"),
    integrations=[AwsLambdaIntegration()],
)


class IdenticalDuplicateUPRNError(Exception):
    pass


class ConflictingDuplicateUPRNError(Exception):
    pass


logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


def check_duplicate_uprns(
    first_letter_data: polars.DataFrame, first_letter: str
) -> polars.DataFrame:
    """
    Check for duplicate UPRNs in the data.

    If there are no duplicate UPRNs then return the data unchanged.
    If all duplicates are identical rows, deduplicate, report to Sentry and return deduplicated dataframe.
    If duplicates have conflicting data, raise ConflictingDuplicateUPRNError.

    """
    duplicated_rows = first_letter_data.filter(
        polars.col("uprn").is_duplicated()
    )
    duplicated_uprn_count = duplicated_rows["uprn"].n_unique()

    if duplicated_uprn_count == 0:
        return first_letter_data

    logger.warning(
        f"check_duplicate_uprns: {first_letter=} {len(first_letter_data)=} {duplicated_uprn_count=}"
    )
    logger.warning(f"duplicated uprns:\n{duplicated_rows.sort('uprn')}")

    if duplicated_uprn_count == 1:
        msg = f"{duplicated_uprn_count} UPRN has duplicated rows for first_letter={first_letter}"
    else:
        msg = f"{duplicated_uprn_count} UPRNs have duplicate rows for first_letter={first_letter}"
    unique_rows = first_letter_data.unique()

    if unique_rows["uprn"].n_unique() == len(unique_rows):
        # All duplicate rows are identical, safe to deduplicate
        msg += (
            " All duplicates are identical. Deduplicating and continuing."
            " The data is probably correct, but we want to understand why this happened."
        )

        logger.error(msg)
        with sentry_sdk.new_scope() as scope:
            scope.set_context(
                "duplicate_uprns",
                {
                    "first_letter": first_letter,
                    "duplicated_uprn_count": duplicated_uprn_count,
                },
            )
            scope.capture_exception(IdenticalDuplicateUPRNError(msg))
        return unique_rows

    # Some duplicate UPRNs have conflicting data
    with sentry_sdk.new_scope() as scope:
        scope.set_context(
            "duplicate_uprns",
            {
                "first_letter": first_letter,
                "duplicated_uprn_count": duplicated_uprn_count,
            },
        )
        msg += (
            " Duplicate uprns are not in identical rows."
            " This indicates a data integrity issue that needs investigation."
        )
        raise ConflictingDuplicateUPRNError(msg)


def handler(event, context):
    # Add CloudWatch log link to Sentry context
    # https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
    region = os.environ.get("AWS_REGION", "eu-west-2")
    log_group = getattr(context, "log_group_name", None)
    log_stream = getattr(context, "log_stream_name", None)
    if log_group and log_stream:
        encoded_group = urllib.parse.quote(log_group, safe="")
        encoded_stream = urllib.parse.quote(log_stream, safe="")
        cloudwatch_url = (
            f"https://{region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={region}#logsV2:log-groups/log-group/{encoded_group}"
            f"/log-events/{encoded_stream}"
        )
        sentry_sdk.set_context(
            "cloudwatch",
            {
                "url": cloudwatch_url,
                "log_group": log_group,
                "log_stream": log_stream,
                "request_id": getattr(context, "aws_request_id", None),
            },
        )

    # Get parameters from the event.
    first_letter = event["first_letter"]
    source_bucket_name = event["source_bucket_name"]
    source_path = event["source_path"]
    dest_bucket_name = event["dest_bucket_name"]
    dest_path = event["dest_path"]
    filter_column = event["filter_column"]

    prefix = f"{source_path}first_letter={first_letter}"

    response = s3_client.list_objects_v2(
        Bucket=source_bucket_name, Prefix=prefix
    )

    # Check if there are any objects returned.
    if "Contents" not in response:
        print(f"No objects found in s3://{source_bucket_name}/{prefix}")
        return

    # Loop through each object and download it to /tmp.
    LOCAL_SOURCE_DIR = Path(f"/tmp/{filter_column}/{first_letter}")
    if LOCAL_SOURCE_DIR.exists():
        shutil.rmtree(LOCAL_SOURCE_DIR)

    LOCAL_SOURCE_DIR.mkdir(exist_ok=True, parents=True)

    BY_OUTCODE_PATH = Path(f"/tmp/by_outcodes/{filter_column}/{first_letter}")
    if BY_OUTCODE_PATH.exists():
        shutil.rmtree(BY_OUTCODE_PATH)
    BY_OUTCODE_PATH.mkdir(exist_ok=True, parents=True)

    for obj in response["Contents"]:
        key = obj["Key"]
        # Use the basename of the key as the local filename.
        local_file = LOCAL_SOURCE_DIR / os.path.basename(key)
        if not local_file.exists():
            print(
                f"Downloading s3://{source_bucket_name}/{key} to {local_file}"
            )
            s3_client.download_file(source_bucket_name, key, local_file)

    print(f"{LOCAL_SOURCE_DIR}/*")
    first_letter_data = polars.read_parquet(f"{LOCAL_SOURCE_DIR}/*")

    first_letter_data = check_duplicate_uprns(first_letter_data, first_letter)

    first_letter_data = first_letter_data.with_columns(
        polars.col("postcode").str.split(" ").list.first().alias("outcode")
    )

    outcode_dfs = first_letter_data.partition_by("outcode")

    expr_has_non_null_filter_column = (
        polars.col(filter_column)
        .list.eval(polars.element().is_not_null())
        .list.sum()
        .alias(f"has_non_null_{filter_column}")
    )

    for outcode_df in outcode_dfs:
        outcode = outcode_df["outcode"][0]
        print(outcode)

        has_any_non_null_filter_column_df = outcode_df.select(
            (expr_has_non_null_filter_column > 0)
            .any()
            .alias(f"any_row_has_{filter_column}")
        )
        has_any_non_null_filter_column = has_any_non_null_filter_column_df[
            f"any_row_has_{filter_column}"
        ][0]  # Boolean True/False

        outcode_path = BY_OUTCODE_PATH / f"{outcode}.parquet"

        if has_any_non_null_filter_column:
            print(
                f"At least one UPRN in {outcode} has data in {filter_column}, writing a file with data"
            )
            outcode_df.sort(by=["postcode", "uprn"])
            outcode_df.write_parquet(outcode_path)
        else:
            print(
                f"No {filter_column} for any address in {outcode}, writing an empty file"
            )
            polars.DataFrame().write_parquet(outcode_path)
        s3_client.upload_file(
            outcode_path, dest_bucket_name, f"{dest_path}/{outcode}.parquet"
        )


if __name__ == "__main__":
    event = {
        "first_letter": "A",
        "source_bucket_name": "pollingstations.private.data",
        "source_path": "addressbase/development/current_ballots_joined_to_address_base/",
        "dest_bucket_name": "dc-data-baker-results-bucket",
        "dest_path": "current_election_parquet",
        "filter_column": "ballot_ids",
    }
    print(event)
    handler(event, {})
