#!/usr/bin/env bash
#
# Build the AGS artifacts that the shared-workflows artifacts-cicd pipeline publishes:
#   - the bulk-loader uber JAR  (com.aerospike:aerospike-graph-bulk-loader; the shade plugin
#     replaces the main artifact, so there is no classifier)
#   - the packaged graphservice Helm chart
#
# Emits ONLY those two into the output directory (default: dist/). The shade plugin also leaves
# the pre-shade thin jar as "original-*.jar"; we deliberately copy only the uber jar so the thin
# jar is never published.
#
# Usage: scripts/build-ags-artifacts.sh <version> [out-dir]
#   <version>  release version (e.g. derived from the git tag). Applied to the poms, the uber jar,
#              and the chart so every artifact shares one version. No manual pom edit is needed:
#              versions:set rewrites the reactor transiently in the CI checkout.
set -euo pipefail

VERSION="${1:?usage: scripts/build-ags-artifacts.sh <version> [out-dir]}"
OUT_DIR="${2:-dist}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BULK_LOADER_JAR="${ROOT_DIR}/aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-${VERSION}.jar"

mkdir -p "${OUT_DIR}"

echo "==> Setting reactor version to ${VERSION}"
mvn -B -q -f "${ROOT_DIR}/pom.xml" \
  versions:set -DnewVersion="${VERSION}" -DprocessAllModules=true -DgenerateBackupPoms=false

echo "==> Building the bulk-loader uber JAR (-am builds the aerospike-graph-gremlin dependency)"
mvn -B -q -f "${ROOT_DIR}/pom.xml" -pl aerospike-graph-bulk-loader -am -DskipTests package

if [[ ! -f "${BULK_LOADER_JAR}" ]]; then
  echo "ERROR: expected uber jar not found: ${BULK_LOADER_JAR}" >&2
  exit 1
fi

echo "==> Collecting the uber JAR into ${OUT_DIR} (the original/thin jar is left behind)"
cp "${BULK_LOADER_JAR}" "${OUT_DIR}/"

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
