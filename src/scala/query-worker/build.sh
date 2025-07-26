#!/bin/sh

set -e

export GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
export GIT_SHA=$(git rev-parse --short HEAD)

../gradlew clean build

docker build --pull -f Dockerfile.local -t public.ecr.aws/cardinalhq.io/lakerunner/query-worker:$GIT_BRANCH_NAME-$GIT_SHA .
docker push public.ecr.aws/cardinalhq.io/lakerunner/query-worker:$GIT_BRANCH_NAME-$GIT_SHA
