#!/usr/bin/env bash
AS_VERSION=${1:-"ee-7.0.0.5_1"}
if [ ! -f .github/aerospike/features.conf ]; then
  if [ -z "$AEROSPIKE_FEATURES_B64" ]; then
    echo "no features file present at .github/aerospike/features.conf"
  else
    echo $AEROSPIKE_FEATURES_B64 | base64 -d > .github/aerospike/features.conf
  fi
else
  virtualenv -p $(which python3) .github/aerospike/venv
  source .github/aerospike/venv/bin/activate
  pip3 install -r .github/aerospike/requirements.txt
  python3 .github/aerospike/start_cluster.py --aerospike_version $AS_VERSION --features_file $(realpath .github/aerospike/features.conf) --repo_path $(realpath ./) $@
fi
