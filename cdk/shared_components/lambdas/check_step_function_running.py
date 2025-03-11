import boto3


def handler(event, context):
    state_machine_arn = event["stateMachineArn"]
    current_execution_arn = event["currentExecutionArn"]

    client = boto3.client("stepfunctions")

    # List all running executions for the given state machine
    response = client.list_executions(
        stateMachineArn=state_machine_arn, statusFilter="RUNNING"
    )
    executions = response.get("executions", [])

    # Filter out the current execution
    other_executions = [
        exe
        for exe in executions
        if exe["executionArn"] != current_execution_arn
    ]

    if other_executions:
        # Another execution is running
        return {
            "proceed": False,
            "message": "Another execution is already running",
        }
    # Safe to proceed
    return {"proceed": True, "message": "No other executions running"}
