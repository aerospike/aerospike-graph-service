#!/usr/bin/env bash
# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ ! -f .github/aerospike/features.conf ]; then
  if [ -z "$AEROSPIKE_FEATURES_B64" ]; then
    echo "no AEROSPIKE_FEATURES_B64 env or features file present at .github/aerospike/features.conf"
    exit
  else
    echo $AEROSPIKE_FEATURES_B64 | base64 -d > .github/aerospike/features.conf
  fi
fi
python3 -m venv .github/aerospike/venv
source .github/aerospike/venv/bin/activate
pip3 install -r .github/aerospike/requirements.txt
python3 .github/aerospike/start_cluster.py --features_file $(realpath .github/aerospike/features.conf) --default_ttl 1 --repo_path $(realpath ./) $@

