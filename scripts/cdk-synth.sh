#!/bin/bash
set -euxo pipefail

# Echo environment information
echo "Running CDK synth with DC_ENVIRONMENT=$DC_ENVIRONMENT"

# Check if CDK is available in node_modules
if [ -f "./node_modules/.bin/cdk" ]; then
  echo "Using CDK from node_modules"
  uv run ./node_modules/.bin/cdk synth "$@"
else
  echo "Error: CDK not found in node_modules. Make sure it's installed with 'npm ci'"
  exit 1
fi
