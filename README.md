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
