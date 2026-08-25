"""
This stack orchestrates the current boundary changes workflow by running the
precursor CSV generation and downstream addressbase-join state machines, followed by a
the final merge step. The result is the creation and population of an S3 bucket
with a parquet file per outcode, containing a list of UK legislation and
their corresponding boundary changes per UK address (UPRN).
"""

from typing import List

from aws_cdk import (
    # CfnOutput,
    Fn,
)
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from shared_components.constructs.singleton_state_machine_construct import (
    SingletonStateMachineConstruct,
)
from shared_components.models import GlueTable, S3Bucket
from stacks.base_stack import DataBakerStack


class CurrentBoundaryChangesCoordinatorStack(DataBakerStack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        current_boundary_changes_precursor_csvs_state_machine_arn = (
            Fn.import_value("MakeCurrentBoundaryChangesPrecursorCSVsArn")
        )

        run_precursor_csv_state_machine_task = (
            self.make_run_state_machine_task_from_arn(
                current_boundary_changes_precursor_csvs_state_machine_arn,
                "MakeCurrentBoundaryChangesPrecursorCSVs",
            )
        )

        current_division_boundary_changes_state_machine_arn = Fn.import_value(
            "MakeCurrentDivisionBoundaryChangesParquetArn"
        )

        current_pre_division_boundary_reviews_state_machine_arn = (
            Fn.import_value("MakeCurrentPreDivisionBoundaryReviewsParquetArn")
        )
        current_boundary_changes_merge_state_machine_arn = Fn.import_value(
            "MakeCurrentBoundaryChangesMergeArn"
        )
        run_merge_state_machine_task = (
            self.make_run_state_machine_task_from_arn(
                current_boundary_changes_merge_state_machine_arn,
                "MakeCurrentBoundaryChangesMerge",
            )
        )

        boundary_changes_ab_join_state_machines = [
            (
                current_division_boundary_changes_state_machine_arn,
                "MakeCurrentDivisionBoundaryChanges",
            ),
            (
                current_pre_division_boundary_reviews_state_machine_arn,
                "MakeCurrentPreDivisionBoundaryReviews",
            ),
        ]

        run_ab_join_state_machines_in_parallel = sfn.Parallel(
            self,
            "Run addressbase join state machines in parallel",
            comment="Run the addressbase join state machines in parallel to create intermediary parquet files",
        )
        for sm_arn, sm_id in boundary_changes_ab_join_state_machines:
            run_state_machine_branch = (
                self.make_run_state_machine_branch_from_arn(sm_arn, sm_id)
            )
            run_ab_join_state_machines_in_parallel.branch(
                run_state_machine_branch
            )

        main_tasks = run_precursor_csv_state_machine_task.next(
            run_ab_join_state_machines_in_parallel
        ).next(run_merge_state_machine_task)

        self.step_function = SingletonStateMachineConstruct(
            self,
            "MakeCurrentBoundaryChangesCoordinatorStack",
            step_function_name="CurrentBoundaryChangesCoordinatorStack",
            main_tasks=main_tasks,
        ).entry_point

        # CfnOutput(
        #     self,
        #     "MakeCurrentBoundaryChangesParquetArnOutput",
        #     value=self.step_function.state_machine_arn,
        #     export_name="MakeCurrentBoundaryChangesParquetArn",
        # )

    @staticmethod
    def s3_buckets() -> List[S3Bucket]:
        return []

    @staticmethod
    def glue_tables() -> List[GlueTable]:
        return []

    def make_run_state_machine_branch_from_arn(
        self, state_machine_arn: str, state_machine_id: str
    ) -> sfn.Chain:
        run_state_machine_task = self.make_run_state_machine_task_from_arn(
            state_machine_arn, state_machine_id
        )
        return sfn.Chain.start(run_state_machine_task)

    def make_run_state_machine_task_from_arn(
        self,
        state_machine_arn: str,
        state_machine_id: str,
    ) -> tasks.StepFunctionsStartExecution:
        state_machine = sfn.StateMachine.from_state_machine_arn(
            self,
            state_machine_id,
            state_machine_arn,
        )
        return tasks.StepFunctionsStartExecution(
            self,
            f"Run{state_machine_id}",
            state_machine=state_machine,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )
