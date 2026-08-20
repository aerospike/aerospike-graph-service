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

#
# The classifier is tested separately. These cover the seam: whether the workflow
# consuming it routes on every value the classifier can emit.

setup() {
    WORKFLOW="${BATS_TEST_DIRNAME}/../.github/workflows/publish-ags-artifacts.yml"
    CLASSIFY="${BATS_TEST_DIRNAME}/classify-release-change.sh"
}

gate() {
    yq -r ".jobs[\"$1\"].if // \"\"" "$WORKFLOW"
}

@test "the classifier can emit a compound mode" {
    run bash -c "printf 'helm/aerospike-graph/values.yaml\ndocker/Dockerfile' | '$CLASSIFY'"
    [ "$output" = "chart container" ]
}

# `== 'container'` drops the compound mode, skipping the rebuild the run was triggered for.
@test "no chain gates on mode by equality" {
    local job
    for job in jars-build jars-fetch chart-build; do
        [[ "$(gate "$job")" != *"outputs.mode =="* ]]
    done
}

@test "every chain head gates on the classified mode" {
    local job
    for job in jars-build jars-fetch chart-build; do
        [[ "$(gate "$job")" == *"needs.version.outputs.mode"* ]]
    done
}

@test "a rebuild reaches the fetch chain" {
    [[ "$(gate jars-fetch)" == *"contains("*"'container'"* ]]
}

@test "a chart re-release reaches the chart chain" {
    [[ "$(gate chart-build)" == *"contains("*"'chart'"* ]]
}

# JARs are only rebuilt when source changed, which forces a version bump.
@test "no partial mode reaches the JAR chain" {
    local g
    g="$(gate jars-build)"
    [[ "$g" == *'"release", "dev"'* ]]
    [[ "$g" != *"'chart'"* ]]
    [[ "$g" != *"'container'"* ]]
}

# It takes same-run JARs on a release and fetched JARs on a rebuild, so it cannot
# gate on mode; it gates on which producer actually ran.
@test "the container accepts either JAR source" {
    local g
    g="$(gate container)"
    [[ "$g" == *"needs.jars-sign.result"* ]]
    [[ "$g" == *"needs.jars-fetch.result"* ]]
}

@test "bundles are created only for a tagged release" {
    local job
    for job in jars-bundle chart-bundle container-bundle; do
        [[ "$(gate "$job")" == *"is-release == 'true'"* ]]
    done
}

# A skip propagates downstream, so a job whose chain contains a skipped ancestor is
# skipped too unless it breaks the propagation. jars-fetch is skipped on every release.
@test "jobs downstream of the container break the skip propagation" {
    local job
    for job in smoke-test container-bundle; do
        [[ "$(gate "$job")" == *"always()"* ]]
    done
}
