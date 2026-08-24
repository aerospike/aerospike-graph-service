#!/usr/bin/env bats
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

setup() {
    SCRIPT="${BATS_TEST_DIRNAME}/build-ags-chart.sh"
    CHART_SRC="${BATS_TEST_DIRNAME}/../helm/aerospike-graph"
}

break_apiversion() {
    local dest="${BATS_TEST_TMPDIR}/chart" template="$1"
    rm -rf "$dest"; mkdir -p "$dest"
    cp -r "$CHART_SRC" "$dest/"
    sed -i "s|^{{ if |{{- if |" "${dest}/aerospike-graph/templates/${template}"
    echo "${dest}/aerospike-graph"
}

@test "the shipped chart packages and every manifest validates" {
    run "$SCRIPT" 3.3.1 3.7.0 "${BATS_TEST_TMPDIR}/ok"
    [ "$status" -eq 0 ]
    [ -f "${BATS_TEST_TMPDIR}/ok/aerospike-graph-3.7.0.tgz" ]
}

@test "a swallowed apiVersion in a default template fails the build" {
    local chart
    chart="$(break_apiversion serviceaccount.yaml)"
    CHART_DIR="$chart" run "$SCRIPT" 3.3.1 3.7.0 "${BATS_TEST_TMPDIR}/bad1"
    [ "$status" -ne 0 ]
    [[ "$output" == *"ServiceAccount"* ]]
}

# ingress is disabled by default, so only the optional-resources render reaches it.
@test "a swallowed apiVersion in an optional template fails the build" {
    local chart
    chart="$(break_apiversion ingress.yaml)"
    CHART_DIR="$chart" run "$SCRIPT" 3.3.1 3.7.0 "${BATS_TEST_TMPDIR}/bad2"
    [ "$status" -ne 0 ]
    [[ "$output" == *"Ingress"* ]]
}
