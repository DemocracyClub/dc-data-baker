import boto3


def handler(event, context):
    if len(event["Records"]) != 1:
        return {"message": "More than one object creation event sent"}
    record = event["Records"][0]
    if record["eventSource"] != "aws:s3":
        return {"message": "Not an s3 Event"}

    if not record["eventName"].startswith("ObjectCreated"):
        return {"message": "Not an object creation event"}

    # Do checks on addressbase_bucket and key??
    # addressbase_bucket = record["s3"]["addressbase_bucket"]["name"]
    # key = record["s3"]["key"]

    # Invoke query
    athena_client = boto3.client("athena")
    workgroup = "dc-data-baker"
    database = "dc_data_baker"

    query_id = get_query(
        athena_client,
        "partition-addressbase-cleaned-query",
        workgroup,
    )
    return athena_client.start_query_execution(
        QueryExecutionId=query_id,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )

    # Update glue table description after query has run - this should probably be another lambda...


def get_query(client, name, workgroup):
    query_ids = client.list_named_queries(WorkGroup=workgroup)["NamedQueryIds"]
    for qid in query_ids:
        query = client.get_named_query(NamedQueryId=qid)
        if name == query["NamedQuery"]["Name"]:
            return query
    return None


# {'NamedQuery': {'Name': 'partition-addressbase-cleaned-query',
#  'Database': 'dc_data_baker',
#  'QueryString': "\n        UNLOAD (SELECT\n\tsplit_part(postcode, ' ', 1) as outcode,\n\tuprn,\n\taddress,\n\tpostcode,\n\tST_X(ST_GeometryFromText(split_part(location, ';', 2))) as longitude,\n\tST_Y(ST_GeometryFromText(split_part(location, ';', 2))) as latitude,\n\tsubstr(postcode, 1,1) as first_letter\nFROM addressbase_cleaned_raw\n\n) \n        TO 's3://pollingstations.private.data/addressbase/testing/addressbase_partitioned/' \n        WITH(\n            format = 'PARQUET',\n            compression = 'SNAPPY',\n            partioned_by = ARRAY['first_letter']            \n        )\n        ",
#  'NamedQueryId': '3a08bb81-4049-4825-a952-aeaf1b94c6f1',
#  'WorkGroup': 'data-baker-workgroup'},
# 'ResponseMetadata': {'RequestId': '50e5e5bc-a7da-42b7-97d1-e46e7a5f4644',
#  'HTTPStatusCode': 200,
#  'HTTPHeaders': {'date': 'Fri, 26 Apr 2024 06:16:35 GMT',
#   'content-type': 'application/x-amz-json-1.1',
#   'content-length': '780',
#   'connection': 'keep-alive',
#   'x-amzn-requestid': '50e5e5bc-a7da-42b7-97d1-e46e7a5f4644'},
#  'RetryAttempts': 0}}
