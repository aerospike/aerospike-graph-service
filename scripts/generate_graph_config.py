import json
import sys


def main(argv):
    aerospike_name = argv[1]
    config_file = argv[2]
    hosts = 'aerospike.client.host='
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
        properties.write('aerospike.client.namespace=test\n')
        properties.write('aerospike.graph.index.vertex.label.enabled=true\n')

if __name__ == "__main__":
    main(sys.argv)

