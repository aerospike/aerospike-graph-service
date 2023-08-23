#!/bin/bash

# Run this script as:
# source ./find-release-candidate.sh docker manifest inspect ghcr.io/citrusleaf/firefly:1.0.0
# and it will set the environment variable DOCKER_RC_TAG to the last release candidate tag that exists

COUNTER=1
while [ $? -eq 0 ]; do
    eval "$@"-rc$COUNTER
    if [ $? -eq 0 ];
    then
        export DOCKER_RC_TAG=$COUNTER
    else
        return 0
    fi
    let COUNTER=COUNTER+1
done
