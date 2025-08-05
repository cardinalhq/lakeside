#!/bin/sh

set -e

export GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
export GIT_SHA=$(git rev-parse --short HEAD)

../gradlew clean build

docker buildx build \
  --pull \
  --push \
  --platform linux/amd64,linux/arm64 \
  -t public.ecr.aws/cardinalhq.io/lakerunner/query-api:latest-dev .
