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

# Install Maven and Docker
sudo apt -y update
sudo apt -y install openjdk-17-jdk
sudo apt -y install maven
sudo apt -y install git

working_dir=$(pwd)

git clone https://github.com/aerospike-community/tinkerbench
cd tinkerbench
git switch json-output

mvn clean install -DskipTests
java -Dconfig=$working_dir/tinkerbench-config.properties -jar ./target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar BenchmarkShortRead

