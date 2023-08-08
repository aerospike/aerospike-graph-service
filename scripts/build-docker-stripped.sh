#!/usr/bin/env bash
set -e
set -o pipefail
DOCKERFILE_A=${DOCKERFILE_A:-"docker/build-stripped.Dockerfile"}
DOCKERFILE_B=${DOCKERFILE_B:-"docker/prod-stripped.Dockerfile"}



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

BUILD_IMAGE="aerospike-graph-stripped-build:latest"
SQUASH_IMAGE="aerospike-graph-stripped-squash:latest"
#docker buildx create --use --name mybuilder

#do the initial build
docker buildx build $EXTRA_BUILD_ARGS --platform "$PLATFORM" --tag $BUILD_IMAGE  --output=type=docker -f $DOCKERFILE_A .

#instantiate container and get container id
CTR_ID=$(docker run -d -t -i --entrypoint=/bin/echo $BUILD_IMAGE)

#export container filesystem to new image stripping historic layers
NEW_IMAGE=$(docker export "$CTR_ID" | docker import -)
docker tag "$NEW_IMAGE" "$SQUASH_IMAGE"
docker images
#add the runtime configuration to the stripped image
docker build $EXTRA_BUILD_ARGS -f $DOCKERFILE_B --tag "$OUTPUT_TAG" .
if [[ -n "$PUSH_FLAG" ]]; then
  docker push $OUTPUT_TAG
fi
