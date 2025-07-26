#!/bin/sh

set -e

export GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
export GIT_SHA=$(git rev-parse --short HEAD)

../gradlew clean build

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile.local \
  -t public.ecr.aws/cardinalhq.io/lakerunner/query-worker:latest .

docker push public.ecr.aws/cardinalhq.io/lakerunner/query-worker:latest
