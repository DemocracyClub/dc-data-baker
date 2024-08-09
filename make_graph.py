import sys

sys.path.append("cdk")

from graphviz import Digraph
from layers.buckets import BUCKETS
from layers.databases import DATABASES
from layers.tables import TABLES

# Create a Digraph object
dot = Digraph()

s3_cluster = Digraph(name="cluster_S3")
s3_cluster.attr(label="S3 Buckets")
s3_cluster.attr(style="filled", color="lightgrey", fontsize="16")

for bucket in BUCKETS:
    s3_cluster.node(bucket.bucket_name, shape="cylinder")

dot.subgraph(s3_cluster)

queries_subgraph = Digraph(name="cluster_queries")
queries_subgraph.attr(label="Named Queries")
queries_subgraph.attr(style="filled", color="lightgrey", fontsize="16")

database_clusters = {}
for database in DATABASES:
    database_clusters[database.database_name] = Digraph(
        name=f"cluster_{database.database_name}"
    )
    database_clusters[database.database_name].attr(
        label=f"Database: {database.database_name}"
    )
    database_clusters[database.database_name].attr(
        style="filled", color="lightgrey", fontsize="16"
    )

# Add nodes and edges based on dependencies
for table in TABLES:
    database_clusters[table.database.database_name].node(
        table.table_name, shape="component"
    )
    if table.depends_on:
        for dep in table.depends_on:
            dot.edge(table.table_name, dep.table_name)
    dot.edge(table.table_name, table.bucket.bucket_name, label=table.s3_prefix)

    if table.populated_with:
        queries_subgraph.node(table.populated_with.name)
        dot.edge(table.table_name, table.populated_with.name)
        if "from_table" in table.populated_with.context:
            dot.edge(
                table.populated_with.name,
                table.populated_with.context["from_table"],
            )

for database_cluster in database_clusters.values():
    dot.subgraph(database_cluster)

# Render the graph
dot.render("databaker", format="png", cleanup=True)
