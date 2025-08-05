#!/usr/bin/bash

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
  java -jar ./target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar --url "$url" --query "$queries" --count "$query_count" --warmup "$warmup_count"
fi

