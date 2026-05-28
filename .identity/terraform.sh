#!/bin/bash

set -e

ACTION=$1
ENV=$2
shift 2
other="$@"

BACKEND_CONFIG_PATH="./env/${ENV}/backend.tfvars"

if [ -z "$ACTION" ]; then
  echo "[ERROR] Missed ACTION: init, apply, plan"
  exit 1
fi

if [ -z "$ENV" ]; then
  echo "[ERROR] ENV should be: dev, uat or prod."
  exit 1
fi

# Source environment-specific config
source "./env/$ENV/backend.ini"

# Set Azure subscription
az account set -s "${subscription}"

# Export Terraform variables
export TF_VAR_github_token="${GITHUB_TOKEN}"

if [ -z "$GITHUB_TOKEN" ]; then
  echo "[ERROR] Set environment variable GITHUB_TOKEN with your GitHub PAT Token"
  exit 1
fi

# Execute Terraform
if echo "init plan apply refresh import output state taint destroy" | grep -w "$ACTION" > /dev/null; then
  if [ "$ACTION" = "init" ]; then
    echo "[INFO] terraform init on ENV: ${ENV}"
    terraform "$ACTION" -backend-config="${BACKEND_CONFIG_PATH}" $other
  elif [ "$ACTION" = "output" ] || [ "$ACTION" = "state" ] || [ "$ACTION" = "taint" ]; then
    terraform init -reconfigure -backend-config="${BACKEND_CONFIG_PATH}"
    terraform "$ACTION" $other
  else
    echo "[INFO] terraform init on ENV: ${ENV}"
    terraform init -reconfigure -backend-config="${BACKEND_CONFIG_PATH}"

    echo "[INFO] terraform ${ACTION} on ENV: ${ENV}"
    terraform "${ACTION}" -var-file="./env/${ENV}/terraform.tfvars" -compact-warnings $other
  fi
else
    echo "[ERROR] ACTION not allowed."
    exit 1
fi

