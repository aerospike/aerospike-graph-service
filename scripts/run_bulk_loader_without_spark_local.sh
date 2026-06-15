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

# Reproduce the CI job "Bulk Loader Call without Spark Fails" locally.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

FIRELY_IMAGE="${FIREFLY_IMAGE:-aerospike/firefly}"
TEST_CLASS="${TEST_CLASS:-TestFireflyBulkLoaderCallEntrypointRemoteFailsWithoutSpark}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_AEROSPIKE="${SKIP_AEROSPIKE:-0}"

cleanup() {
  local ids
  ids="$(docker ps -q --filter ancestor="$FIRELY_IMAGE" 2>/dev/null || true)"
  if [[ -n "$ids" ]]; then
    echo "Stopping Firefly containers: $ids"
    docker stop $ids >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_cmd docker
require_cmd mvn
require_cmd python3

if [[ ! -f .github/aerospike/features.conf && -z "${AEROSPIKE_FEATURES_B64:-}" ]]; then
  cat <<'EOF'
Aerospike enterprise license is required for the test cluster.

Provide one of:
  export AEROSPIKE_FEATURES_B64='<base64 license from CI secret AEROSPIKE_DEV_LICENSE>'
  cp /path/to/features.conf .github/aerospike/features.conf

Or, if Aerospike is already running on port 3000:
  SKIP_AEROSPIKE=1 ./scripts/run_bulk_loader_without_spark_local.sh
EOF
  if [[ "$SKIP_AEROSPIKE" != "1" ]]; then
    exit 1
  fi
fi

if [[ "$SKIP_BUILD" != "1" ]]; then
  echo "==> Building Maven artifacts..."
  mvn package -pl aerospike-graph-gremlin -am -DskipTests --no-transfer-progress -q
  echo "==> Installing python_on_whales if needed..."
  pip3 install --user -q python_on_whales 2>/dev/null || pip3 install -q python_on_whales
  echo "==> Building slim Firefly Docker image..."
  python3 ./scripts/build-docker.py --tags "$FIRELY_IMAGE" --slim --use_local
fi

if [[ "$SKIP_AEROSPIKE" != "1" ]]; then
  echo "==> Starting Aerospike test cluster (single node)..."
  bash scripts/start_aerospike.sh --node_count 1
fi

echo "==> Starting slim Firefly container..."
docker run -d -p 8182:8182 \
  --add-host=host.docker.internal:host-gateway \
  -e aerospike.client.host=host.docker.internal \
  -e FIREFLY_REPO_ROOT="$REPO_ROOT" \
  -v "$REPO_ROOT/conf/docker-bulk-load/:/opt/aerospike-graph/custom/" \
  -v "$REPO_ROOT/data/docker-bulk-load/:/opt/aerospike-graph/etc/" \
  "$FIRELY_IMAGE"

echo "==> Waiting for Gremlin Server..."
bash scripts/wait_for_gremlin_server.sh

echo "==> Gremlin host for tests: ${FIREFLY_DOCKER_GREMLIN_HOST:-localhost}"
echo "==> Running Maven test $TEST_CLASS..."
mvn test -pl aerospike-graph-gremlin -Dtest="$TEST_CLASS" --no-transfer-progress

echo "==> Local reproduction succeeded."
