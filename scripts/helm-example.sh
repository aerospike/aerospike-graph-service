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

if [[ "$1" = "" ]]; then
  echo "Provide helm action (install | upgrade)"
elif [[ "$2" = "" ]]; then
  echo "Provide pod name"
elif [[ "$3" = "" ]]; then
  echo "Provide replica count"
elif [[ "$4" = "" ]]; then
  echo "Provide aerospike host"
elif [[ "$5" = "" ]]; then
  echo "Provide aerospike namespace"
else
  ACTION="$1"
  POD_NAME="$2"
  REPLICA_COUNT="$3"
  AEROSPIKE_HOST="$4"
  AEROSPIKE_NS="$5"
  helm "$ACTION" "$POD_NAME" helm/graphservice \
    --set "env[0].name=aerospike.client.host" \
    --set "env[0].value=$AEROSPIKE_HOST" \
    --set "env[1].name=aerospike.client.namespace" \
    --set "env[1].value=$AEROSPIKE_NS" \
    --set "replicaCount=$REPLICA_COUNT"
fi
