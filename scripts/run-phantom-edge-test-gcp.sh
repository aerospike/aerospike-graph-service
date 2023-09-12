#!/usr/bin/bash

set -eo pipefail

# Install Maven and Docker
sudo apt -y update
sudo apt -y install maven
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Extract Firefly Repo
sudo tar -zxvf firefly.tgz

# Build and Run Firefly Docker Image
cd firefly && sudo bash -x scripts/build-docker.sh firefly
sudo docker run --name firefly -d -p 8182:8182 -p 9090:9090 -e aerospike.client.host="$(cat ./scripts/hosts.txt)" -e aerospike.graph.index.vertex.label.enabled=true -e aerospike.graph.index.vertex.properties=macAddress firefly

# Run Benchmark
sudo mvn test -Dfirefly.host=localhost -Dtest=TestPhantomEdges -DfailIfNoTests=false --no-transfer-progress
