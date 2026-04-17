#!/bin/bash
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

# Run this script as:
# source ./find-release-candidate.sh docker manifest inspect ghcr.io/aerospike/aerospike-graph-service:1.0.0
# and it will set the environment variable DOCKER_RC_TAG to the last release candidate tag that exists

COUNTER=1
while [ $? -eq 0 ]; do
    eval "$@"-rc$COUNTER
    if [ $? -eq 0 ];
    then
        export DOCKER_RC_TAG=$COUNTER
    else
        return 0
    fi
    let COUNTER=COUNTER+1
done
