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

# Packages the aerospike-graph Helm chart alone, without building the JARs.
#
# Do not swap kubeconform for helm lint: both it and helm template exit 0 when
# a {{- trim swallows a document's apiVersion.
#
# Usage: scripts/build-ags-chart.sh <app-version> <chart-version> [out-dir]
set -euo pipefail

APP_VERSION="${1:?usage: scripts/build-ags-chart.sh <app-version> <chart-version> [out-dir]}"
CHART_VERSION="${2:?usage: scripts/build-ags-chart.sh <app-version> <chart-version> [out-dir]}"
OUT_DIR="${3:-dist}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${CHART_DIR:-${ROOT_DIR}/helm/aerospike-graph}"
# Schemas are version-specific, so leaving this unpinned validates against
# whatever kubeconform defaults to rather than what the clusters run.
KUBE_VERSION="${KUBE_VERSION:-1.30.0}"

# Not on GitHub runners, and execute-build has no input that installs it.
KUBECONFORM_VERSION="${KUBECONFORM_VERSION:-v0.7.0}"
KUBECONFORM_SHA256="c31518ddd122663b3f3aa874cfe8178cb0988de944f29c74a0b9260920d115d3"
if ! command -v kubeconform >/dev/null 2>&1; then
  echo "==> Installing kubeconform ${KUBECONFORM_VERSION}"
  curl -fsSL -o /tmp/kubeconform.tgz \
    "https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/kubeconform-linux-amd64.tar.gz"
  echo "${KUBECONFORM_SHA256}  /tmp/kubeconform.tgz" | sha256sum -c -
  tar xzf /tmp/kubeconform.tgz -C /tmp kubeconform
  sudo install -m 0755 /tmp/kubeconform /usr/local/bin/kubeconform
fi

mkdir -p "${OUT_DIR}"

echo "==> Packaging the aerospike-graph chart ${CHART_VERSION} for AGS ${APP_VERSION}"
helm package "${CHART_DIR}" \
  --version "${CHART_VERSION}" --app-version "${APP_VERSION}" --destination "${OUT_DIR}"

CHART_TMP="$(mktemp -d)"
trap 'rm -rf "${CHART_TMP}"' EXIT
tar xzf "${OUT_DIR}/aerospike-graph-${CHART_VERSION}.tgz" -C "${CHART_TMP}"

echo "==> Validating rendered manifests against Kubernetes ${KUBE_VERSION}"
helm template smoke "${CHART_TMP}/aerospike-graph" \
  | kubeconform -strict -summary -kubernetes-version "${KUBE_VERSION}"

# Ingress and autoscaling default off, so nothing else ever renders them.
helm template smoke "${CHART_TMP}/aerospike-graph" \
  --set ingress.enabled=true --set autoscaling.enabled=true \
  | kubeconform -strict -summary -kubernetes-version "${KUBE_VERSION}"

RENDERED="$(helm template smoke "${CHART_TMP}/aerospike-graph" | grep -oE 'image: "[^"]+"' | head -1)"
case "${RENDERED}" in
  *":${APP_VERSION}\""*) echo "==> Chart renders ${RENDERED}" ;;
  *)
    echo "ERROR: chart renders ${RENDERED}, expected the image tag to be ${APP_VERSION}" >&2
    exit 1
    ;;
esac

ls -1 "${OUT_DIR}"
