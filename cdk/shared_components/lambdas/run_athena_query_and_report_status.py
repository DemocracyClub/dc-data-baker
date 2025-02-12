"""
A generic Lambda that can run an Athena query and wait for it to complete

"""

import time

import boto3

athena_client = boto3.client("athena")


def get_named_query_by_name(query_name: str) -> dict:
    """
    Athena stores UUIDs and names for queries.

    There is no way in the API to get a query by a name. `get_named_query`
    takes a UUID, not a query name, annoyingly.

    So, we need to get all the saved queries and return the first one
    where the name matches.

    """

    # Get a list of all named query IDs in the workgroup
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

    # Raise if no query found
    raise ValueError(f"Query {query_name} not found")


def handler(event, context):
    """
    Supports both starting and then checking an Athena query.

    Everything is configured by keys in the `event` dict.

    `QueryName` is the named query to run. The query gets the event dict
                as a template context

    `QueryString` if this is passed in, `QueryName` is ignored. Designed
                  to allow running ad-hox queries rather than saved queries.

    `context`: this is passed to the query and anything here can be used
               with `$foo` template substitution.

    If `blocking` is passed then we run the Lambda until the query finished.

    If `queryExecutionId` is passed in, then we check for the status of a
    previously started query. This is used by AWS Step Functions to test if a
    long-running job has finished.

    """
    print(event)
    if "queryExecutionId" in event:
        # Check query status
        response = athena_client.get_query_execution(
            QueryExecutionId=event["queryExecutionId"]
        )
        status = response["QueryExecution"]["Status"]["State"]
        return {"status": status}

    query_string = event.get("QueryString", None)
    if not query_string:
        saved_query_name = event["QueryName"]
        response = get_named_query_by_name(saved_query_name)
        query_string = response["QueryString"]

    # The query can contain $foo template strings that are replaced with
    # items in event["context"]
    formatted_query = query_string.format(**event.get("context", {}))

    start_response = athena_client.start_query_execution(
        QueryString=formatted_query,
        QueryExecutionContext={"Database": "dc_data_baker"},
        WorkGroup="dc-data-baker",
        # TODO: do we need this?
        # ResultConfiguration={
        #     "OutputLocation": "s3://dc-data-baker-results-bucket/uprn-to-ballots-first-letter/"
        # },
    )
    if not event.get("blocking"):
        return {"queryExecutionId": start_response["QueryExecutionId"]}

    while True:
        # Wait a little on each iteration
        time.sleep(1)
        response = athena_client.get_query_execution(
            QueryExecutionId=start_response["QueryExecutionId"]
        )
        status = response["QueryExecution"]["Status"]
        state = status["State"]

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            if state != "SUCCEEDED":
                # This might contain useful debugging info
                error_reason = status.get("StateChangeReason")
                print(f"Query did not succeed: {error_reason}")
                raise ValueError(f"Query did not succeed: {error_reason}")
            break

    return {"queryExecutionId": start_response["QueryExecutionId"]}


if __name__ == "__main__":
    print(handler({"first_letter": "S"}, {}))
