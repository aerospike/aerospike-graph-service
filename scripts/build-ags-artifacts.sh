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
#
# Build the AGS artifacts that the shared-workflows artifacts-cicd pipeline publishes:
#   - the graph uber JAR        (com.aerospike:aerospike-graph-gremlin; carries the runtime
#     entrypoint com.aerospike.firefly.runtime.FireflyServer)
#   - the bulk-loader uber JAR  (com.aerospike:aerospike-graph-bulk-loader; the shade plugin
#     replaces the main artifact, so there is no classifier)
#   - the packaged graphservice Helm chart
#
# Emits ONLY those three into the output directory (default: dist/). Both modules shade, so each
# also leaves a pre-shade thin jar as "original-*.jar"; we deliberately copy only the uber jars so
# no thin jar is ever published.
#
# The graph JAR is collected because the container images need it and must not compile their own.
# It is already built here as a transitive dependency of the bulk-loader (-am).
#
# Usage: scripts/build-ags-artifacts.sh <version> [out-dir]
#   <version>  release version (e.g. derived from the git tag). Applied to the poms, the uber jar,
#              and the chart so every artifact shares one version. No manual pom edit is needed:
#              versions:set rewrites the reactor transiently in the CI checkout.
set -euo pipefail

VERSION="${1:?usage: scripts/build-ags-artifacts.sh <version> [out-dir]}"
OUT_DIR="${2:-dist}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GRAPH_JAR="${ROOT_DIR}/aerospike-graph-gremlin/target/aerospike-graph-gremlin-${VERSION}.jar"
BULK_LOADER_JAR="${ROOT_DIR}/aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-${VERSION}.jar"

mkdir -p "${OUT_DIR}"

echo "==> Setting reactor version to ${VERSION}"
mvn -B -q -f "${ROOT_DIR}/pom.xml" \
  versions:set -DnewVersion="${VERSION}" -DprocessAllModules=true -DgenerateBackupPoms=false

echo "==> Building the bulk-loader uber JAR (-am builds the aerospike-graph-gremlin dependency)"
mvn -B -q -f "${ROOT_DIR}/pom.xml" -pl aerospike-graph-bulk-loader -am -DskipTests package

for jar in "${GRAPH_JAR}" "${BULK_LOADER_JAR}"; do
  if [[ ! -f "${jar}" ]]; then
    echo "ERROR: expected uber jar not found: ${jar}" >&2
    exit 1
  fi
done

echo "==> Collecting the uber JARs into ${OUT_DIR} (the original/thin jars are left behind)"
cp "${GRAPH_JAR}" "${BULK_LOADER_JAR}" "${OUT_DIR}/"

# The container images load their entrypoint from the graph JAR, so verify it is present here
# rather than discovering it in a published image.
#
# Matched with a case statement rather than a pipe to grep -q: under `set -o pipefail`, grep -q
# closes the pipe on its first match, unzip takes SIGPIPE, and a successful match reads as a
# failed pipeline.
GRAPH_ENTRIES="$(unzip -Z1 "${OUT_DIR}/$(basename "${GRAPH_JAR}")")"
case "${GRAPH_ENTRIES}" in
  *"com/aerospike/firefly/runtime/FireflyServer.class"*) ;;
  *)
    echo "ERROR: ${GRAPH_JAR} does not contain com/aerospike/firefly/runtime/FireflyServer.class" >&2
    exit 1
    ;;
esac

echo "==> Packaging the graphservice Helm chart at version ${VERSION}"
helm package "${ROOT_DIR}/helm/graphservice" \
  --version "${VERSION}" --app-version "${VERSION}" --destination "${OUT_DIR}"

echo "==> Artifacts emitted to ${OUT_DIR}:"
ls -1 "${OUT_DIR}"

# Guard: the thin (original-) jar must never reach the artifact directory.
if ls "${OUT_DIR}"/original-*.jar >/dev/null 2>&1; then
  echo "ERROR: a thin/original jar leaked into ${OUT_DIR}" >&2
  exit 1
fi
