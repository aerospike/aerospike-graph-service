#!/usr/bin/env bash
set -e
set -o pipefail
GRAPH_JAR=aerospike-graph-gremlin/target/aerospike-graph-gremlin-2.4.0-SNAPSHOT.jar
BULK_LOADER_JAR=aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-2.4.0-SNAPSHOT.jar

OUTPUT_TAG="$1"
if [[ -z "$OUTPUT_TAG" ]]; then
    echo "Usage: $0 <tag> [platform] [--push]"
    exit 1
fi

PLATFORM="$2"
if [[ -z "$PLATFORM" ]]; then
    PLATFORM="linux/amd64"
fi

PUSH_FLAG="$3"
if [[ -z "$PUSH_FLAG" ]]; then
    PUSH_FLAG=""
fi

mvn -pl aerospike-graph-gremlin -pl aerospike-graph-bulk-loader -am -DskipTests=true clean install --no-transfer-progress

docker buildx build $EXTRA_BUILD_ARGS --output=type=docker -f "docker/Dockerfile-stripped" . \
  --platform "$PLATFORM" \
  --tag "$OUTPUT_TAG" \
  --build-arg FIREFLY_GRAPH="$GRAPH_JAR" --build-arg BULKLOADER="$BULK_LOADER_JAR"

if [[ -n "$PUSH_FLAG" ]]; then
  docker push $OUTPUT_TAG
fi
