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

set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-8182}"
TIMEOUT="${3:-180}"
SETTLE_SECONDS="${4:-15}"

echo "Waiting up to ${TIMEOUT}s for Gremlin Server on ${HOST}:${PORT}..."
for ((second = 1; second <= TIMEOUT; second++)); do
  if (echo > "/dev/tcp/${HOST}/${PORT}") 2>/dev/null; then
    echo "Gremlin Server port open after ${second}s; waiting ${SETTLE_SECONDS}s for startup to finish"
    sleep "$SETTLE_SECONDS"
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for Gremlin Server on ${HOST}:${PORT}"
container_id="$(docker ps -aq --filter publish="${PORT}" | head -1 || true)"
if [[ -n "$container_id" ]]; then
  docker logs "$container_id" 2>&1 | tail -100 || true
fi
exit 1
