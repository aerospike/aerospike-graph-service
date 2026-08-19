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

# One case per row of the trigger table, plus the mixed source cases that row
# ordering exists to disambiguate.

setup() {
    CLASSIFY="${CLASSIFY:-${BATS_TEST_DIRNAME}/classify-release-change.sh}"
}

# No trailing newline, which is what exercises the classifier's EOF guard.
classify() {
    printf '%s' "$1" | "$CLASSIFY"
}

# --- Row 5: nothing changed -------------------------------------------------

@test "no changes at all falls back to a dev build" {
    run classify ""
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

# --- Row 3: chart alone -----------------------------------------------------

@test "a chart change alone releases the chart" {
    run classify "helm/aerospike-graph/values.yaml"
    [ "$status" -eq 0 ]
    [ "$output" = "chart" ]
}

# --- Row 4: each container path alone ---------------------------------------

@test "a Dockerfile change alone rebuilds the container" {
    run classify "docker/Dockerfile"
    [ "$status" -eq 0 ]
    [ "$output" = "container" ]
}

@test "a runtime script change alone rebuilds the container" {
    run classify "docker-runtime-scripts/firefly-server.sh"
    [ "$status" -eq 0 ]
    [ "$output" = "container" ]
}

@test "a conf change alone rebuilds the container" {
    run classify "conf/logback.xml"
    [ "$status" -eq 0 ]
    [ "$output" = "container" ]
}

# --- Row 2: chart and container together, order-independent ------------------

@test "chart plus container releases both" {
    run classify "helm/aerospike-graph/values.yaml
docker/Dockerfile"
    [ "$status" -eq 0 ]
    [ "$output" = "chart container" ]
}

@test "the same pair in the opposite order releases both" {
    run classify "docker/Dockerfile
helm/aerospike-graph/values.yaml"
    [ "$status" -eq 0 ]
    [ "$output" = "chart container" ]
}

# --- Row 1: source outranks everything --------------------------------------

@test "a source change alone falls back to a dev build" {
    run classify "aerospike-graph-gremlin/src/Main.java"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "a pom change counts as source" {
    run classify "pom.xml"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "an .mvn config change counts as source" {
    run classify ".mvn/wrapper/maven-wrapper.properties"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "source beats a chart change rather than releasing the chart" {
    run classify "aerospike-graph-gremlin/src/Main.java
helm/aerospike-graph/values.yaml"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "source beats a container change rather than rebuilding the container" {
    run classify "pom.xml
docker/Dockerfile"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "source beats both together" {
    run classify "aerospike-graph-olap/pom.xml
helm/aerospike-graph/Chart.yaml
docker/Dockerfile-slim"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

# --- Paths matching no row release nothing on their own ---------------------

@test "a docs change releases nothing" {
    run classify "docs/README.md"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "an unrelated workflow change releases nothing" {
    run classify ".github/workflows/test-extended.yml"
    [ "$status" -eq 0 ]
    [ "$output" = "dev" ]
}

@test "an unrelated path does not suppress a chart release" {
    run classify "docs/README.md
helm/aerospike-graph/values.yaml"
    [ "$status" -eq 0 ]
    [ "$output" = "chart" ]
}

# --- Input shape ------------------------------------------------------------

@test "a trailing newline does not duplicate or drop the last path" {
    run bash -c "printf 'docs/README.md\nhelm/aerospike-graph/values.yaml\n' | '$CLASSIFY'"
    [ "$status" -eq 0 ]
    [ "$output" = "chart" ]
}

@test "a blank line among the paths is ignored" {
    run classify "docs/README.md

helm/aerospike-graph/values.yaml"
    [ "$status" -eq 0 ]
    [ "$output" = "chart" ]
}
