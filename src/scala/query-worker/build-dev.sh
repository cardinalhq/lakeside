#!/bin/sh
# Copyright (C) 2025 CardinalHQ, Inc
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -e

export GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
export GIT_SHA=$(git rev-parse --short HEAD)

../gradlew clean build

docker buildx build \
  --pull \
  --push \
  --platform linux/amd64,linux/arm64 \
  -t public.ecr.aws/cardinalhq.io/lakerunner/query-worker:latest-dev .
