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

import json
import sys


def main(argv):
    ags_image = argv[1]
    ghcr_io_login = argv[2]
    graph_file = argv[3]

    with open(graph_file, 'w') as properties:
        properties.write("#!/usr/bin/bash\n")
        properties.write("set -eo pipefail\n")
        properties.write("sudo apt -y update\n")
        properties.write("sudo apt -y install docker.io\n")
        properties.write(f"echo \"running with {ghcr_io_login}\"\n")
        properties.write(f"echo {ghcr_io_login} | sudo docker login ghcr.io -u aerobot-firefly --password-stdin\n")
        properties.write(f"echo \"Logged in successfully. Attempting to start {ags_image}\"\n")
        properties.write(f"sudo docker run --name firefly -d -p 8182:8182 -p 9090:9090 -v $(pwd)/graph-config.properties:/opt/aerospike-graph/aerospike-graph.properties {ags_image}\n")

if __name__ == "__main__":
    main(sys.argv)

