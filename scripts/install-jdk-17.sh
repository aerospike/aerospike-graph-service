#!/usr/bin/bash

set -eo pipefail

# This file needs to be manually uploaded to gs://gha-ci-firefly-bulkloader/scripts/install-jdk-17.sh if modified
sudo apt -y update
sudo apt -y install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
