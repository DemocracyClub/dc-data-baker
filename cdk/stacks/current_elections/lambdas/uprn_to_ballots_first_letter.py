import time

import boto3

athena_client = boto3.client("athena")


def get_named_query_by_name(query_name):
    print(f"LOOKING UP QUERY NAME {query_name}")

    # Get a list of all named query IDs (handle pagination if needed)
    response = athena_client.list_named_queries(WorkGroup="dc-data-baker")
    named_query_ids = response.get("NamedQueryIds", [])

    # Iterate over each ID and retrieve the details
    for query_id in named_query_ids:
        query_details = athena_client.get_named_query(
            NamedQueryId=query_id,
        )
        named_query = query_details.get("NamedQuery", {})
        if named_query.get("Name") == query_name:
            return named_query

    raise ValueError(f"Query {query_name} not found")


def handler(event, context):
    if "queryExecutionId" not in event:
        saved_query_name = "uprn-to-ballots-first-letter.sql"
        response = get_named_query_by_name(saved_query_name)

        query_string = response["QueryString"]
        formatted_query = query_string.format(**event)

        start_response = athena_client.start_query_execution(
            QueryString=formatted_query,
            QueryExecutionContext={"Database": "dc_data_baker"},
            ResultConfiguration={
                "OutputLocation": "s3://dc-data-baker-results-bucket/uprn-to-ballots-first-letter/"
            },
        )
        print(start_response)

        while True:
            time.sleep(2)
            response = athena_client.get_query_execution(
                QueryExecutionId=start_response["QueryExecutionId"]
            )
            status = response["QueryExecution"]["Status"]
            state = status["State"]

            print(f"Current query state: {state}")

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                if state != "SUCCEEDED":
                    # This might contain useful debugging info
                    error_reason = status.get("StateChangeReason")
                    print(f"Query did not succeed: {error_reason}")
                    raise ValueError(f"Query did not succeed: {error_reason}")
                break

            # Wait a bit before checking again

        print(response)
        return {"queryExecutionId": start_response["QueryExecutionId"]}

        # Check query status
    response = athena_client.get_query_execution(
        QueryExecutionId=event["queryExecutionId"]
    )
    status = response["QueryExecution"]["Status"]["State"]

    return {"status": status}


if __name__ == "__main__":
    print(handler({"first_letter": "S"}, {}))
