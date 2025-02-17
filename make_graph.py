import os
import sys

sys.path.append("cdk")
from aws_cdk import Stack
from constructs import Construct
from graphviz import Digraph
from shared_components.databases import DEFAULT_DATABASE
from stacks.addressbase import AddressBaseStack

stacks = [AddressBaseStack]

# Create a Digraph object
dot = Digraph()
dot.attr(rankdir="TB")

all_s3_buckets = []


def get_app():
    os.environ["DC_ENVIRONMENT"] = "development"
    # import here as we need to set the env first
    from app import app  # noqa

    return app


cdk_app = get_app()


class StepFunctionGraph:
    def __init__(self, stack, definition):
        self.stack_name = stack.__class__.__name__
        states_map = self.gather_states(stack)
        visited_states = self.traverse_state(
            stack.state_definition.start_state, states_map
        )
        full_definition = {
            "StartAt": definition.start_state.node.id,
            "States": visited_states,
        }

        self.definition = full_definition
        self.states = full_definition.get("States", {})
        self.start_state = full_definition.get("StartAt")
        if not self.start_state:
            raise ValueError(
                "State machine definition must include a 'StartAt' field."
            )

    def gather_states(self, root: Construct):
        state_mapping = {}
        for child in root.node.find_all():
            # We assume a state will have a to_state_json method.
            if hasattr(child, "to_state_json"):
                state_mapping[child.node.id] = child
        return state_mapping

    # Helper function: traverse from a starting state following "Next" transitions.
    def traverse_state(self, state, state_mapping, visited=None):
        if visited is None:
            visited = {}
        state_id = state.node.id
        if state_id in visited:
            return visited
        state_json = state.to_state_json()
        visited[state_id] = state_json
        # If there's a linear transition, follow it.
        if "Next" in state_json:
            next_state_id = state_json["Next"]
            next_state = state_mapping.get(next_state_id)
            if next_state:
                self.traverse_state(next_state, state_mapping, visited)
        # (Extend here for Choice, Parallel, Map, etc.)
        return visited

    def build_graph(self, graph) -> Digraph:
        self._process_state(self.start_state, graph)
        return graph

    def _process_state(self, state_name: str, graph: Digraph,
                       visited: set = None):
        if visited is None:
            visited = set()
        if state_name in visited:
            return
        visited.add(state_name)

        state = self.states.get(state_name)
        if not state:
            return
        state_type = state.get("Type", "Unknown")

        # Create the prefixed node id
        prefixed_state_name = f"{self.stack_name}_{state_name}"

        # Add a node for this state with the prefixed id.
        graph.node(
            prefixed_state_name,
            label=f"{self.stack_name}: {state_name}\n({state_type})",
        )

        # Handle a linear "Next" transition.
        if "Next" in state:
            next_state = state["Next"]
            # Compute the prefixed name for the next state as well.
            prefixed_next_state = f"{self.stack_name}_{next_state}"
            graph.edge(prefixed_state_name, prefixed_next_state)
            self._process_state(next_state, graph, visited)
        elif state.get("End", False):
            # Terminal state—no outgoing edge.
            pass


for stack in cdk_app.node.children:
    if not isinstance(stack, Stack):
        continue
    stack_name = stack.__class__.__name__
    stack_cluster = Digraph(name=f"cluster_{stack_name}")
    stack_cluster.attr(label=stack_name)
    stack_cluster.attr(style="filled", color="lightgrey", fontsize="16")
    stack_cluster.attr(newrank="true", rankdir="TB")

    if hasattr(stack, "state_definition"):
        step_function_cluster = Digraph(
            name=f"cluster_{stack_name}_step_function"
        )
        step_function_cluster.attr(label=f"Step Functions: {stack_name}")
        step_function_cluster.attr(
            style="filled", color="darkgrey", fontsize="16"
        )
        step_function_cluster.attr('node', shape='rectangle')
        step_function_cluster.attr(newrank="true", rankdir="TB")
        step_function_graph_maker = StepFunctionGraph(
            stack, stack.state_definition
        )
        step_function_cluster = step_function_graph_maker.build_graph(
            step_function_cluster
        )
        stack_cluster.subgraph(step_function_cluster)

    database_cluster = Digraph(name=f"cluster_{DEFAULT_DATABASE.database_name}")
    database_cluster.attr(label=f"Database: {DEFAULT_DATABASE.database_name}")
    database_cluster.attr(style="filled", color="darkgrey", fontsize="16")

    queries_subgraph = Digraph(name=f"cluster_{stack_name}_queries")
    queries_subgraph.attr(label=f"{stack_name} Queries")
    queries_subgraph.attr(style="filled", color="darkgrey", fontsize="16")

    all_s3_buckets += stack.s3_buckets()

    for table in stack.glue_tables():
        stack_cluster.node(table.table_name, shape="component")
        if table.depends_on:
            for dep in table.depends_on:
                dot.edge(table.table_name, dep.table_name)
        dot.edge(
            table.bucket.bucket_name, table.table_name, label=table.s3_prefix
        )

        if table.populated_with:
            queries_subgraph.node(table.populated_with.name)
            dot.edge(table.populated_with.name, table.table_name)
            if "from_table" in table.populated_with.context:
                dot.edge(
                    table.populated_with.context["from_table"],
                    table.populated_with.name,
                )

    stack_cluster.subgraph(queries_subgraph)

    dot.subgraph(stack_cluster)

s3_cluster = Digraph(name="cluster_S3")
s3_cluster.attr(label="S3 Buckets")
s3_cluster.attr(style="filled", color="lightgrey", fontsize="16")

for bucket in all_s3_buckets:
    s3_cluster.node(bucket.bucket_name, shape="cylinder")
dot.subgraph(s3_cluster)

# Render the graph
dot.render("databaker", format="png", cleanup=True)
