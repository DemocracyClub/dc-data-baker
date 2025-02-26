import boto3


def delete_all_objects_with_prefix(bucket_name, prefix):
    """
    Delete all objects at a prefix, dealing with paging over a large
    number of keys.

    """

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    keys_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            keys_to_delete.append({"Key": obj["Key"]})

    for i in range(0, len(keys_to_delete), 1000):
        batch = keys_to_delete[i : i + 1000]
        response = s3.delete_objects(
            Bucket=bucket_name, Delete={"Objects": batch}
        )
        print("Deleted batch:", response)


def handler(event, context):
    bucket_name = event["bucket"]
    prefix = event["prefix"]
    delete_all_objects_with_prefix(bucket_name, prefix)
