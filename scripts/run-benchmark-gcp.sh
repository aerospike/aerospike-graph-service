#!/usr/bin/bash

set -eo pipefail

# Install Maven and Docker
sudo apt -y update
sudo apt -y install openjdk-17-jdk
sudo apt -y install maven
sudo apt -y install python3-pip
pip3 install python_on_whales

curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
mkdir -p ~/.docker/cli-plugins
curl -SL https://github.com/docker/buildx/releases/download/v0.18.0/buildx-v0.18.0.linux-amd64 -o ~/.docker/cli-plugins/docker-buildx
chmod +x ~/.docker/cli-plugins/docker-buildx

# Extract Firefly Repo
sudo tar -zxvf firefly.tgz

# Build and Run Firefly Docker Image
sudo docker buildx create --use --driver docker-container
cd firefly && sudo python3 scripts/build-docker.py --tags firefly
sudo docker run --name firefly -d -p 8182:8182 -p 9090:9090 -e aerospike.client.host="$(cat ./scripts/hosts.txt)" -e aerospike.graph.index.vertex.label.enabled=true -e aerospike.graph.index.vertex.properties=macAddress firefly

# Run Benchmark
sudo mvn clean install -DskipTests
sudo mvn test -pl aerospike-graph-gremlin -Dfirefly.host=localhost -Ddataset.size="$(cat ./data_size.txt)" -Dstorage.type="$(cat ./storage_type.txt)" -Dtest="$(cat ./benchmark_name.txt)"  --no-transfer-progress
