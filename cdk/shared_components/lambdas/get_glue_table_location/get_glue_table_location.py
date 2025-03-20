import boto3


def handler(event, context):
    database = event["database"]
    table = event["table"]

    glue_client = boto3.client("glue")

    response = glue_client.get_table(DatabaseName=database, Name=table)
    location = response["Table"]["StorageDescriptor"]["Location"]

    return {f"{table}_location": location}


if __name__ == "__main__":
    print(
        handler(
            {"database": "dc_data_baker", "table": "addressbase_cleaned_raw"},
            {},
        )
    )
