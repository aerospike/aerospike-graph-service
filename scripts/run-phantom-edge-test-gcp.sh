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
sudo apt -y install python3-pip
pip3 install python_on_whales --break-system-packages --ignore-installed

curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Extract Firefly Repo
sudo tar -zxvf firefly.tgz

# Build and Run Firefly Docker Image
sudo docker buildx create --use --driver docker-container
cd aerospike-graph-service && sudo python3 scripts/build-docker.py --tags firefly
sudo docker run --name firefly -d -p 8182:8182 -p 9090:9090 -e aerospike.client.host="$(cat ./scripts/hosts.txt)" -e aerospike.graph.index.vertex.label.enabled=true -e aerospike.graph.index.vertex.properties=macAddress -e aerospike.client.scan.max.wait=1800000 firefly

# Run Benchmark
sudo mvn clean install -DskipTests
sudo mvn test -pl aerospike-graph-gremlin -Dfirefly.host=localhost -Dtest=TestPhantomEdges  --no-transfer-progress
