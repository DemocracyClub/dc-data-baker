![Code style ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)

# DC Data Baker

This project is about _[denormalizing](
https://en.wikipedia.org/wiki/Denormalization)_ data we hold into a format that 
optimised for _read heavy queries_. 

We know that we need to be able to scale up to a lot of traffic around large 
elections.

Doing, e.g a point-in-polygon query for every postcode search is 
computationally expensive. This expense requires a complex deployment story 
and ends up being risky, with the posibility that problems will only show up 
when under high load. 

To mitigate this, we convert the data into a flattened format. We call this 
'baking'. This is a term we've [borrowed from 3D modelling](
https://blender.stackexchange.com/questions/14416/what-does-baking-mean). We  gain query performance, with some trade-offs.

Because 'baked' data isn't the full data model, we can't perform arbitrary 
queries on the data. A 'baked' layer isn't a replacement for a relational 
database.

We also, in some case, sacrifice some referential integrity: baked data 
might contain IDs to objects that don't actually exist. Client code needs to 
deal with this.

It might be useful to think of a baked layer as a materialised view: some 
queries have been run to make a single table from a relational database. The 
single table is easier to query than performing SQL joins each time.

## Hash lookups

Although baked data can be in more or less any shape, the main use-case is 
for making a 'hash lookup' or mapping (not to be confused with geographical 
maps) between addresses and other IDs.

For example, rather than performing a spatial query to get the current 
elections for each address, we can make a mapping file between each UPRN and 
a list of ballot IDs for that UPRN. At query time we can then look up this 
UPRN in the baked layer. Each ballot object can be loaded from a static 
endpoint.

In this instance, the mapping layer is a parquet file per outcode (the first 
part of the postcode). A parquet file is a way to store tabular data that's 
easy and fast to query. 

## Layers

When we talk about a 'baked layer' we're talking about a single dataset that 
can typically only answer a single question. 

Examples of layers that we might have are:

- Current elections (UPRN to list of ballots)
- Boundary reviews (UPRN to boundary reviews taking place)
- List of organiations (UPRN to all layers of government for that point)
- etc

## Architecture 

We use AWS Athena, AWS Lambda and AWS Step Functions to automate the 
creation of each layer. 

We've taken the decision to put logic inside Lambda functions written in 
Python, even if there's a built-in AWS way to do the same thing. This is an 
attempt to make each layer easier to understand for Python devs. Ideally 
each layer should be understandable by reading the step function code. 

## What this makes

![](databaker.png)

(Update this graph by running `uv run make_graph.png`)

## Install

### Python dependencies

This project uses [`uv`](https://github.com/astral-sh/uv) to manage python packages.
[Install uv](https://docs.astral.sh/uv/getting-started/installation/) first if you don't already have it. Then

```
uv sync
```


### Commit hooks

```shell
pre-commit install
```

### Javascript dependencies

We're using CDK so you need the cdk tooling.

Probably best to use `nvm` to manage `npm` to manage installing libraries.

Then
```shell
nvm use --lts
npm install
```

## CDK Setup

```shell
AWS_PROFILE=dev-aggregatorapi-dc DC_ENVIRONMENT=development cdk bootstrap
```

```shell
AWS_PROFILE=dev-aggregatorapi-dc DC_ENVIRONMENT=development cdk synth
```


# Current layers

The main concept in this repo is that of a 'layer'. This is really an
artifact that is produced by code in this repo that can be use elsewhere.

The two current layers are:

## AddressBase Layer

This is a AWS Glue table with AddressBase in. It's partitioned by first letter
for speedy filtering. It can be used in Athena.

The version of AddressBase is taken from the "addressbase cleaned" file
that's made in the WDIV account. This is a simple format that squashes
address fields into one.

## CurrentElections Layer

Performs a geo-join on the AddressBase layer and a CSV of every ballot ID
and it's division WKT.

At the end of the process, a parquet file per outcode is produced,
containing one row per address and for each, a list of ballot IDs.

This can be used to look up current elections in other applications.

## Adding new layers

A layer is really a CDK stack. To make a new layer, make a stack and drive
AWS in the way you normally would.

Look at other layers for patterns. Generally we use AWS Lambda, Glue, Athena
and Step Functions and S3 to make a ETL pipeline, but your new layer might
need other services.

That being said, there are some handy things you can use:

### `run_athena_query_and_report_status`

This is a Lambda function that will run a named Athena query. Saved writing
a Lambda per query.

### `empty_s3_bucket_by_prefix`

Lambda function that will empty an S3 bucket. This is important because
Athena will query all files at a prefix, including duplicates of the same
file. e.g if you have 5 copies of AddressBase in a prefix, Athena will
return 5 rows per UPRN, or whatever.

It's useful to be able to empty a prefix, and this will do that.

## Adding new state machines.

If you add a new state machine (either in a new layer, or an existing one) you probably want to also do the following:
* export the arn of the statemachine at the end of the stack.
```python
CfnOutput(
    self,
    "MakeAddressBasePartitionedArnOutput",
    value=self.step_function.state_machine_arn,
    export_name="MakeAddressBasePartitionedArn",
)
```
* Update `scripts/check-state-machines-run.py`.
  * Tell the `__init__` method how to find the arn of your new state machine.
  * Add a line to the `handle` method to check your new state machine.
