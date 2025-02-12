import sys

sys.path.append("cdk")
from graphviz import Digraph
from shared_components.databases import DEFAULT_DATABASE
from stacks.addressbase import AddressBaseStack

stacks = [AddressBaseStack]

# Create a Digraph object
dot = Digraph()

all_s3_buckets = []

for stack in stacks:
    stack_name = stack.__name__
    stack_cluster = Digraph(name=f"cluster_{stack_name}")
    stack_cluster.attr(label=stack_name)
    stack_cluster.attr(style="filled", color="lightgrey", fontsize="16")

    database_cluster = Digraph(name=f"cluster_{DEFAULT_DATABASE.database_name}")
    database_cluster.attr(label=f"Database: {DEFAULT_DATABASE.database_name}")
    database_cluster.attr(style="filled", color="lightgrey", fontsize="16")

    queries_subgraph = Digraph(name=f"cluster_{stack_name}_queries")
    queries_subgraph.attr(label=f"{stack_name} Queries")
    queries_subgraph.attr(style="filled", color="lightgrey", fontsize="16")

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

#
# queries_subgraph = Digraph(name="cluster_queries")
# queries_subgraph.attr(label="Named Queries")
# queries_subgraph.attr(style="filled", color="lightgrey", fontsize="16")
#
# database_clusters = {}
# for database in DATABASES:
#     database_clusters[database.database_name] = Digraph(
#         name=f"cluster_{database.database_name}"
#     )
#     database_clusters[database.database_name].attr(
#         label=f"Database: {database.database_name}"
#     )
#     database_clusters[database.database_name].attr(
#         style="filled", color="lightgrey", fontsize="16"
#     )
#
# # Add nodes and edges based on dependencies
# for table in TABLES:
#     database_clusters[table.database.database_name].node(
#         table.table_name, shape="component"
#     )
#     if table.depends_on:
#         for dep in table.depends_on:
#             dot.edge(table.table_name, dep.table_name)
#     dot.edge(table.table_name, table.bucket.bucket_name, label=table.s3_prefix)
#
#     if table.populated_with:
#         queries_subgraph.node(table.populated_with.name)
#         dot.edge(table.table_name, table.populated_with.name)
#         if "from_table" in table.populated_with.context:
#             dot.edge(
#                 table.populated_with.name,
#                 table.populated_with.context["from_table"],
#             )
#
# for database_cluster in database_clusters.values():
#     dot.subgraph(database_cluster)
#
# Render the graph
dot.render("databaker", format="png", cleanup=True)
