#!/usr/bin/env bash
AS_VERSION="ee-7.0.0.3_1"
if [ ! -f .github/aerospike/features.conf ]; then
  echo "no features file present at .github/aerospike/features.conf"
else
  virtualenv -p $(which python3) .github/aerospike/venv
  source .github/aerospike/venv/bin/activate
  pip3 install -r .github/aerospike/requirements.txt
  python3 .github/aerospike/start_cluster.py --single --aerospike_version $AS_VERSION --features_file $(realpath .github/aerospike/features.conf) --repo_path $(realpath ./) $@
fi
