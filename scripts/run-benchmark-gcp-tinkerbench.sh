#!/usr/bin/bash

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

