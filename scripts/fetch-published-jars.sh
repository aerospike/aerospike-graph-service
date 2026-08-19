#!/usr/bin/bash
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

# Downloads published AGS JARs so a container rebuild skips the reactor, which
# cannot reproduce released bytes (no project.build.outputTimestamp).
#
# Usage: scripts/fetch-published-jars.sh <version> [out-dir] [repo]
set -euo pipefail

VERSION="${1:?usage: scripts/fetch-published-jars.sh <version> [out-dir] [repo]}"
OUT_DIR="${2:-dist}"
REPO="${3:-connect-maven-dev-local}"
STAGE_DIR="${OUT_DIR}/unsigned-artifacts"

mkdir -p "${STAGE_DIR}"

echo "==> Fetching AGS ${VERSION} JARs from ${REPO}"
# The .asc files ride along because the Dockerfile copies "${JAR}"* and a glob
# matching nothing fails that COPY.
for artifact in aerospike-graph-gremlin aerospike-graph-bulk-loader; do
  jf rt dl "${REPO}/com/aerospike/${artifact}/${VERSION}/" "${STAGE_DIR}/" \
    --flat=true --fail-no-op
done

ls -1 "${STAGE_DIR}"
