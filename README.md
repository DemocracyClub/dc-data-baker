![Code style ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)

# DC Data Baker

Orchestration for writing files to s3 to power our APIs

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


# Layers

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
