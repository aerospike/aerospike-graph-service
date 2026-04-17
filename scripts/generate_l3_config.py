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
    github_sha = argv[1]
    dataset_size = argv[2]
    benchmark = argv[3]
    hosts = 'aerospike.client.host='
    ip_only = ''
    bucket_root = 'gs://incremental-datasets/' + benchmark + '/' + dataset_size + '/'
    vertices_path = 'aerospike.graphloader.vertices=' + bucket_root + 'vertices'
    edges_path = 'aerospike.graphloader.edges=' + bucket_root + 'edges'
    temp_path = 'aerospike.graphloader.temp-directory=' + 'gs://gha-ci-firefly-bulkloader/' +  github_sha + '/' + 'temp'

    with open('./clusters.json') as file:
        clusters = json.load(file)

        for cluster in clusters:
            if github_sha in cluster['ClusterName']:
                ip_only = ip_only + cluster['PrivateIp'] + ':3000,'
        ip_only = ip_only[:-1]
        hosts = hosts + ip_only

        with open('l3_config_base.properties', 'r') as base_properties:
            with open("l3_config.properties", 'w') as properties:
                properties.write(f'{hosts}\n')
                properties.write(f'{vertices_path}\n')
                properties.write(f'{edges_path}\n')
                properties.write(f'{temp_path}\n')
                properties.write(base_properties.read())
        with open('hosts.txt', 'x') as host_txt:
            host_txt.write(ip_only)


if __name__ == "__main__":
    main(sys.argv)

