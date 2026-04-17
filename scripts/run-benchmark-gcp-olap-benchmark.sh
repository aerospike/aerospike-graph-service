#!/usr/bin/bash
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

set -eo pipefail

if [[ "$1" = "" ]]; then
  echo "Provide url for gremlin-server"
elif [[ "$2" = "" ]]; then
  echo "Provide queries"
elif [[ "$3" = "" ]]; then
  echo "Provide query count"
elif [[ "$4" = "" ]]; then
  echo "Provide warmup count"
else
  url="$1"
  queries="$2"
  query_count="$3"
  warmup_count="$4"

  sudo apt -y update
  sudo apt -y install openjdk-17-jdk
  sudo apt -y install maven
  sudo apt -y install git

  working_dir=$(pwd)

  git clone https://github.com/aerospike-community/tinkerbench
  cd tinkerbench
  git switch olap-benchmark

  mvn clean install -DskipTests -q
  decoded_query=$(echo "${queries}" | base64 -d)

  echo "Running benchmark with url: $url, query: $queries, query count: $query_count, warmup: $warmup_count"
  java -jar ./target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar --url "$url" --query "$decoded_query" --count "$query_count" --warmup "$warmup_count"
fi

