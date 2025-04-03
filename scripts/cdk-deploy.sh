#!/bin/bash
set -euxo pipefail

# Echo environment information
echo "Running CDK synth with DC_ENVIRONMENT=$DC_ENVIRONMENT"

# If AWS_PROFILE exists (+x) then see if it's set.
# Useful if calling script locally.
if [ -n "${AWS_PROFILE+x}" ]; then
  echo "Using AWS_PROFILE=$AWS_PROFILE"
fi

# Check if CDK is available in node_modules
if [ -f "./node_modules/.bin/cdk" ]; then
  echo "Using CDK from node_modules"
  uv run npx --no cdk deploy "$@"
else
  echo "Error: CDK not found in node_modules. Make sure it's installed with 'npm ci'"
  exit 1
fi
