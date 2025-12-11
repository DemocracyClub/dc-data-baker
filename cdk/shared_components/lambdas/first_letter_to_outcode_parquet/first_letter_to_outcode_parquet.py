import os
from pathlib import Path

import boto3
import polars

s3_client = boto3.client("s3")


def handler(event, context):
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
    LOCAL_SOURCE_DIR = Path(f"/tmp/{first_letter}")
    LOCAL_SOURCE_DIR.mkdir(exist_ok=True)

    BY_OUTCODE_PATH = Path(f"/tmp/by_outcodes/{first_letter}")
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
        "first_letter": "G",
        "source_bucket_name": "dc-data-baker-results-bucket",
        "source_path": "current_ballots_joined_to_address_base/",
        "dest_bucket_name": "dc-data-baker-results-bucket",
        "dest_path": "current_election_parquet",
        "filter_column": "ballot_ids",
    }
    print(event)
    handler(event, {})
