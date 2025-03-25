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

    expr_has_non_null_ballots = (
        polars.col("ballot_ids")
        .list.eval(polars.element().is_not_null())
        .list.sum()
        .alias("has_non_null_ballots")
    )

    for outcode_df in outcode_dfs:
        outcode = outcode_df["outcode"][0]
        print(outcode)

        has_any_non_null_ballots_df = outcode_df.select(
            (expr_has_non_null_ballots > 0).any().alias("any_row_has_ballots")
        )
        has_any_non_null_ballots = has_any_non_null_ballots_df[
            "any_row_has_ballots"
        ][0]  # Boolean True/False

        outcode_path = BY_OUTCODE_PATH / f"{outcode}.parquet"

        if has_any_non_null_ballots:
            print(
                f"At least one UPRN in {outcode} has an election, writing an empty file"
            )
            outcode_df.write_parquet(outcode_path)
        else:
            print(
                f"No ballot for any address in {outcode}, writing an empty file"
            )
            polars.DataFrame().write_parquet(outcode_path)
        s3_client.upload_file(
            outcode_path, dest_bucket_name, f"{dest_path}/{outcode}.parquet"
        )


if __name__ == "__main__":
    event = {
        "first_letter": "G",
        "source_bucket_name": "dc-data-baker-results-bucket",
        "source_path": "current_ballots_joined_to_address_base",
        "dest_bucket_name": "dc-data-baker-results-bucket",
        "dest_path": "current_elections_parquet",
    }
    print(event)
    handler(event, {})
