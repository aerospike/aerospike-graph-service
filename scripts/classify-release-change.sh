# Without the `|| [[ -n ${path} ]]`, a final line lacking a newline is dropped.#!/usr/bin/bash
# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Decides what a workflow_dispatch publishes, from the paths changed since the
# last release tag. Reads paths on stdin, writes one of: chart, container,
# "chart container", dev.
#
# First match wins. Source must outrank the rest: a container rebuilt from
# fetched JARs would embed bytes predating the source change.
#
# Usage: git diff --name-only "$(git describe --tags --abbrev=0 --match 'v*')"..HEAD | classify-release-change.sh
set -euo pipefail

has_source=0
has_chart=0
has_container=0

# Without the `|| [[ -n ${path} ]]`, a final line lacking a newline is dropped.
while IFS= read -r path || [[ -n ${path} ]]; do
  [[ -z ${path//[[:space:]]/} ]] && continue
  case "${path}" in
    aerospike-graph-*/* | pom.xml | .mvn/*)
      has_source=1
      ;;
    helm/*)
      has_chart=1
      ;;
    docker/* | docker-runtime-scripts/* | conf/*)
      has_container=1
      ;;
  esac
done

if ((has_source)); then
  echo "dev"
elif ((has_chart && has_container)); then
  echo "chart container"
elif ((has_chart)); then
  echo "chart"
elif ((has_container)); then
  echo "container"
else
  echo "dev"
fi
