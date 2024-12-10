import json
import sys


def main(argv):
    aerospike_name = argv[1]
    benchmark_config_file = argv[2]
    hosts = 'aerospike.client.host='
    bucket_root = 'gs://identity-benchmark/SF1M/'
    vertices_path = 'aerospike.graphloader.vertices=' + bucket_root + 'vertices'
    edges_path = 'aerospike.graphloader.edges=' + bucket_root + 'edges'
    temp_path = 'aerospike.graphloader.temp-directory=' + 'gs://gha-ci-firefly-bulkloader/' +  aerospike_name + '/' + 'temp'
    ip_only = ''
    with open('./clusters.json') as file:
        clusters = json.load(file)

        for cluster in clusters:
            if aerospike_name in cluster['ClusterName']:
                ip_only = ip_only + cluster['PrivateIp'] + ':3000,'
        ip_only = ip_only[:-1]
        hosts = hosts + ip_only

    if hosts == 'aerospike.client.host=':
        print('Cluster not found')
        sys.exit(1)

    with open(benchmark_config_file, 'w') as properties:
        properties.write(f'{hosts}\n')
        properties.write(f'{vertices_path}\n')
        properties.write(f'{edges_path}\n')
        properties.write(f'{temp_path}\n')
        properties.write('aerospike.client.namespace=test\n')
        properties.write('aerospike.graph.index.vertex.label.enabled=true\n')

if __name__ == "__main__":
    main(sys.argv)

