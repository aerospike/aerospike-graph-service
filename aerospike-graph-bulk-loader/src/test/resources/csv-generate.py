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

import csv, sys, os


try:
    os.makedirs('src/test/resources/recoverydata/vertices', exist_ok=True)
    with open('src/test/resources/recoverydata/vertices/vertexList.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',)
        # Loop 10k times
        writer.writerow(['~id', '~label'])
        for i in range(2500000):
            writer.writerow([i, 'vertex'])
    os.makedirs('src/test/resources/recoverydata/edges', exist_ok=True)
    with open('src/test/resources/recoverydata/edges/edgeList.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',)
        # Loop 10k times
        writer.writerow(['~id', '~label', '~from', '~to'])
        for i in range(1000000):
            writer.writerow([i, 'edge', i, 1])
        for i in range(2499999):
            writer.writerow([i + 1000000, 'edge', i, i + 1])
    print("Success")
    sys.exit(0)
except Exception as e:
    sys.exit(1)
