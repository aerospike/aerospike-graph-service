#!/usr/bin/bash

set -eo pipefail

# Install Maven and Docker
sudo apt -y update
sudo apt -y install openjdk-17-jdk
sudo apt -y install maven
sudo apt -y install python3-pip
pip3 install python_on_whales

# Extract Firefly Repo
sudo tar -zxvf firefly.tgz

# Build and Run Firefly Docker Image
cd firefly && sudo python3 scripts/build-docker.py --tags firefly
sudo docker run --name firefly -d -p 8182:8182 -p 9090:9090 -e aerospike.client.host="$(cat ./scripts/hosts.txt)" -e aerospike.graph.index.vertex.label.enabled=true -e aerospike.graph.index.vertex.properties=macAddress -e aerospike.client.scan.max.wait=1800000 firefly

# Run Benchmark
sudo mvn test -Dfirefly.host=localhost -Dtest=TestPhantomEdges -DfailIfNoTests=false --no-transfer-progress
