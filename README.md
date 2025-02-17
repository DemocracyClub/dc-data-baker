![Code style ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)

# DC Data Baker

Orchestration for writing files to s3 to power our APIs

## What this makes

![](databaker.png)

(Update this graph by running `python make_graph.png`)

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


----------

# CDK Documentation

This documentation provides detailed information on the CDK (Cloud Development Kit) setup for deploying various AWS resources. The CDK scripts are organized in several Python modules and scripts that define infrastructure on AWS, primarily using AWS Glue and S3.

## Files and Modules Overview

1. **cdk/app.py**
2. **cdk/layers/** (subpackage)
   - `__init__.py` (initialization)
   - `buckets.py`
   - `databases.py`
   - `models.py`
   - `tables.py`
3. **cdk/stacks/** (subpackage)
   - `__init__.py` (initialization)
   - `data_baker.py`

### `cdk/app.py`

This script initializes the CDK application and sets up the application environment. It determines which AWS account and region to deploy to, configures context settings, and defines tags for all resources.

- **App Initialization**: Creates an AWS CDK app with environment-specific context.
- **Environment Configuration**: Fetches AWS account and region details.
- **Validation**: Ensures that the specified environment (`dc-environment`) is valid.
- **Stack Definition**: Deploys the `DataBakerStack`.
- **Tags**: Tags each resource with `dc-product` and `dc-environment` for organizational purposes.

### `cdk/layers/__init__.py`

An empty file for initializing the `layers` package. It does not contain any specific logic.

### `cdk/layers/buckets.py`

This file defines S3 buckets using a custom `S3Bucket` data class.

- **pollingstations_private_data**: An S3 bucket for storing private data related to polling stations.
- **data_baker_results_bucket**: An S3 bucket designed for storing results from the Data Baker service.
- **BUCKETS List**: A collection of S3 bucket objects to be incorporated into the CDK stack.

### `cdk/layers/databases.py`

Defines Glue databases using a custom `GlueDatabase` data class.

- **dc_data_baker**: A Glue database object representing a database for storing data related to the DC Data Baker service.
- **DATABASES List**: Contains defined database objects for use within the CDK stack.

### `cdk/layers/models.py`

Defines the data model classes using Python data classes.

- **S3Bucket**: Defines the structure for an S3 bucket.
- **GlueDatabase**: Represents the structure of a Glue database.
- **BaseQuery**: Represents a query with a name and context dictionary.
- **BaseTable**: Represents an AWS Glue table definition, including critical attributes such as table name, bucket, prefix, data format, columns, and more. It helps define Glue tables and their relationships.

### `cdk/layers/tables.py`

Defines Glue tables based on the `BaseTable` data class.

- **addressbase_cleaned_raw**: Represents a table with the cleaned Addressbase data stored in CSV format.
- **addressbase_partitioned**: Represents a table with partitioned Addressbase data stored in Parquet format.
- **TABLES List**: A collection of all tables to be used in the CDK stack.

### `cdk/stacks/__init__.py`

An empty file for initializing the `stacks` package. It does not contain any specific logic.

### `cdk/stacks/data_baker.py`

Implements the `DataBakerStack`, which is a collection of AWS resources provisioned together.

- **DataBakerStack Class**: Extends the `Stack` class to provision resources such as Glue databases, S3 buckets, and Athena workgroups.
  - **get_query_text Function**: Reads SQL query text from a file.
  - **make_athena_workgroup Method**: Configures and returns an Athena workgroup for managing query results.
  - **make_databases Method**: Instantiates Glue databases defined in `DATABASES`.
  - **collect_buckets Method**: Imports existing buckets specified in `BUCKETS`.
  - **make_tables Methods**: Creates Glue tables with configurations as specified in `TABLES`. It connects tables with SQL queries using `make_named_query`.
  - **make_named_query Method**: Creates an Athena named query for querying data in Glue tables.

These modules work together to provision an AWS infrastructure comprising Glue tables and databases, Athena workgroups, and S3 buckets, which are integral parts of the data processing pipeline.
