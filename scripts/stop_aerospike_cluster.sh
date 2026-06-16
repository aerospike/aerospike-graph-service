#!/usr/bin/env bash
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

set -euo pipefail

# Match by image name: Docker's ancestor filter does not match tagged images
# such as aerospike/aerospike-server-enterprise:8.1.
mapfile -t aerospike_containers < <(
  docker ps -a --format '{{.ID}} {{.Image}}' \
    | awk '$2 ~ /^aerospike\/aerospike-server-enterprise/ { print $1 }'
)

if ((${#aerospike_containers[@]} > 0)); then
  docker rm -f "${aerospike_containers[@]}"
fi
