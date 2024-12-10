#!/usr/bin/bash

set -eo pipefail

# Install Maven and Docker
sudo apt -y update
sudo apt install docker.io

sudo docker run --name firefly \
  -d -p 8182:8182 -p 9090:9090 \
  -v $(pwd)/graph-config.properties:/opt/aerospike-graph/aerospike-graph.properties \
  $(cat ./ags_image)
