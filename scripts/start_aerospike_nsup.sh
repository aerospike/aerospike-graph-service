#!/usr/bin/env bash
if [ ! -f .github/aerospike/features.conf ]; then
  if [ -z "$AEROSPIKE_FEATURES_B64" ]; then
    echo "no AEROSPIKE_FEATURES_B64 env or features file present at .github/aerospike/features.conf"
    exit
  else
    echo $AEROSPIKE_FEATURES_B64 | base64 -d > .github/aerospike/features.conf
  fi
fi

# todo: replace with public image when available
docker pull aerospike.jfrog.io/docker/aerospike/aerospike-server:8.0.0.0-rc1

virtualenv -p $(which python3) .github/aerospike/venv
source .github/aerospike/venv/bin/activate
pip3 install -r .github/aerospike/requirements.txt
python3 .github/aerospike/start_cluster.py --features_file $(realpath .github/aerospike/features.conf) \
  --node_count 3 \
  --sc --aerospike_version 8.0.0.0-rc1 \
  --config_template aerospike_sc.conf.j2 \
  --repo_path $(realpath ./) $@

