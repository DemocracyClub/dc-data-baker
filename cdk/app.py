#!/usr/bin/env python3

import os

from aws_cdk import App, Environment, Tags
from stacks.addressbase import AddressBaseStack
from stacks.current_boundary_changes_coordinator import (
    CurrentBoundaryChangesCoordinatorStack,
)
from stacks.current_boundary_changes_merge import (
    CurrentBoundaryChangesMergeStack,
)
from stacks.current_boundary_changes_precursor_csvs import (
    CurrentBoundaryChangesPrecursorCSVsStack,
)
from stacks.current_division_boundary_changes import (
    CurrentDivisionBoundaryChangesStack,
)
from stacks.current_elections import CurrentElectionsStack
from stacks.current_pre_division_boundary_reviews import (
    CurrentPreDivisionBoundaryReviewsStack,
)
from stacks.data_baker_core import DataBakerCoreStack

valid_environments = (
    "development",
    "staging",
    "production",
)

app_wide_context = {}
if dc_env := os.environ.get("DC_ENVIRONMENT"):
    app_wide_context["dc-environment"] = dc_env

app = App(context=app_wide_context)

env = Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2")

# Set the DC Environment early on. This is important to be able to conditionally
# change the stack configurations
dc_environment = app.node.try_get_context("dc-environment") or None
assert dc_environment in valid_environments, (
    f"context `dc-environment` must be one of {valid_environments}"
)


DataBakerCoreStack(
    app,
    "DataBakerCoreStack",
    env=env,
)

AddressBaseStack(
    app,
    "AddressBaseStack",
    env=env,
)

CurrentElectionsStack(
    app,
    "CurrentElectionsStack",
    env=env,
)


current_bc_precursor_csv_stack = CurrentBoundaryChangesPrecursorCSVsStack(
    app,
    "CurrentBoundaryChangesPrecursorCSVsStack",
    env=env,
)

current_div_bc_stack = CurrentDivisionBoundaryChangesStack(
    app,
    "CurrentDivisionBoundaryChangesStack",
    env=env,
)

current_pre_div_br_stack = CurrentPreDivisionBoundaryReviewsStack(
    app,
    "CurrentPreDivisionBoundaryReviewsStack",
    env=env,
)

current_bc_merge_stack = CurrentBoundaryChangesMergeStack(
    app,
    "CurrentBoundaryChangesMergeStack",
    env=env,
)

current_bc_coordinator_stack = CurrentBoundaryChangesCoordinatorStack(
    app,
    "CurrentBoundaryChangesCoordinatorStack",
    env=env,
)

current_bc_coordinator_stack.add_dependency(current_bc_precursor_csv_stack)
current_bc_coordinator_stack.add_dependency(current_div_bc_stack)
current_bc_coordinator_stack.add_dependency(current_pre_div_br_stack)
current_bc_coordinator_stack.add_dependency(current_bc_merge_stack)

Tags.of(app).add("dc-product", "dc-data-baker")
Tags.of(app).add("dc-environment", dc_environment)

app.synth()
