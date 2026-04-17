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

#
# Used by the secret-masking integration test.
#
# The properties file at conf/test-secret-masking/aerospike-graph.properties
# and the CI workflow that launches the service both inject the string
# "MASKME" (plus a few variants) as stand-in values for passwords and
# other sensitive configuration. If the masking logic is working
# correctly, none of those raw values should appear in the server's
# docker logs; if any do, the mask leaked and this script fails.
#
# Exit status is the raw match count, so a non-zero exit means "leak
# detected".

set -eo pipefail

echo "Searching for instances of the unmasked test token in container logs..."
count=$(grep -o -c MASKME <(docker logs firefly) || true)
echo "Search complete."
echo "Found $count instances of the unmasked test token."
exit $count
