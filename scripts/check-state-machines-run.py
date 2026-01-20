import argparse
import json
import time
from pathlib import Path

import boto3


class StateMachineFailed(Exception): ...


class CheckStateMachinesRun:
    def __init__(self, cdk_outputs_path):
        self.cdk_output = None
        with Path(cdk_outputs_path).open() as f:
            self.cdk_output = json.load(f)

        self.addressbase_state_machine_arn = (
            self.get_addressbase_state_machine_arn()
        )
        self.current_elections_state_machine_arn = (
            self.get_current_elections_state_machine_arn()
        )

        self.current_boundary_changes_state_machine_arn = (
            self.get_current_boundary_changes_state_machine_arn()
        )
        self.sfn_client = boto3.client("stepfunctions")

    def handle(self):
        print("Checking state-machines run...")
        self.check_state_machine(self.addressbase_state_machine_arn)
        self.check_state_machine(
            self.current_elections_state_machine_arn, timeout=120
        )
        self.check_state_machine(
            self.current_boundary_changes_state_machine_arn, timeout=240
        )

    def get_addressbase_state_machine_arn(self):
        addressbase_outputs = self.cdk_output["AddressBaseStack"]
        return addressbase_outputs["MakeAddressBasePartitionedArnOutput"]

    def get_current_elections_state_machine_arn(self):
        current_elections_outputs = self.cdk_output["CurrentElectionsStack"]
        return current_elections_outputs["MakeCurrentElectionsParquetArnOutput"]

    def get_current_boundary_changes_state_machine_arn(self):
        current_boundary_changes_outputs = self.cdk_output[
            "CurrentBoundaryChangesStack"
        ]
        return current_boundary_changes_outputs[
            "MakeCurrentBoundaryChangesParquetArnOutput"
        ]

    def check_state_machine(self, state_machine_arn, timeout=60):
        print(f"Checking {state_machine_arn}")
        execution_arn = self.execute_stepfunction(state_machine_arn)
        status, response = self.check_execution_status(
            execution_arn, timeout=timeout
        )

        if status == "SUCCEEDED":
            print(f"{state_machine_arn} run successfully.")
        else:
            print(f"{state_machine_arn} run failed.")
            print(response)
            raise StateMachineFailed

    def execute_stepfunction(self, state_machine_arn):
        """Execute a Step Function and return the execution ARN."""
        response = self.sfn_client.start_execution(
            stateMachineArn=state_machine_arn
        )
        return response["executionArn"]

    def check_execution_status(
        self, execution_arn, timeout=60, poll_interval=5
    ):
        """Check the status of a Step Function execution with timeout."""
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            response = self.sfn_client.describe_execution(
                executionArn=execution_arn
            )
            status = response["status"]

            if status == "RUNNING":
                continue

            if status == "SUCCEEDED":
                return status, response

            if status not in ["SUCCEEDED", "RUNNING"]:
                return status, response

            time.sleep(poll_interval)

        return "METHOD TIMEOUT", {
            "message": "Custom timeout hit, go check the console to see what's happened"
        }


def main():
    parser = argparse.ArgumentParser(
        description="Check and execute AWS Step Functions state machines."
    )
    parser.add_argument(
        "cdk_outputs_path",
        help="Path to the CDK output file containing state machine ARNs",
    )
    args = parser.parse_args()
    checker = CheckStateMachinesRun(args.cdk_outputs_path)
    checker.handle()


if __name__ == "__main__":
    main()
