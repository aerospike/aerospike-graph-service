#!/usr/bin/env bash
if [[ "$1" = "" ]]; then
  echo "Provide helm action (install | upgrade)"
elif [[ "$2" = "" ]]; then
  echo "Provide pod name"
elif [[ "$3" = "" ]]; then
  echo "Provide replica count"
elif [[ "$4" = "" ]]; then
  echo "Provide aerospike host"
elif [[ "$5" = "" ]]; then
  echo "Provide aerospike namespace"
else
  ACTION="$1"
  POD_NAME="$2"
  REPLICA_COUNT="$3"
  AEROSPIKE_HOST="$4"
  AEROSPIKE_NS="$5"
  helm "$ACTION" "$POD_NAME" helm/graphservice \
    --set "env[0].name=aerospike.client.host" \
    --set "env[0].value=$AEROSPIKE_HOST" \
    --set "env[1].name=aerospike.client.namespace" \
    --set "env[1].value=$AEROSPIKE_NS" \
    --set "replicaCount=$REPLICA_COUNT"
fi
