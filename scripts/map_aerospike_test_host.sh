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

# Map integration-test hostname to the Docker bridge IP (shell redirects bypass sudo).
if ! grep -q 'aerospike.test.aerospike.dev' /etc/hosts; then
  if echo "172.17.0.1 aerospike.test.aerospike.dev" | sudo tee -a /etc/hosts > /dev/null 2>&1; then
    :
  else
    echo "Warning: could not update /etc/hosts (sudo required). Tests using aerospike.test.aerospike.dev may fail locally."
  fi
fi
