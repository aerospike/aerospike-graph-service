import json
import sys


def main(argv):
    aerospike_graph_name = argv[1]
    benchmark_config_file = argv[2]
    hosts = 'graph.server.host='
    ip_only = ''
    with open('./clients.json') as file:
        clusters = json.load(file)

        for cluster in clusters:
            if aerospike_name in cluster['ClusterName']:
                ip_only = ip_only + cluster['PrivateIp']
        ip_only = ip_only[:-1]
        hosts = hosts + ip_only

    if hosts == 'graph.server.host=':
        print('Cluster not found')
        sys.exit(1)

    with open(benchmark_config_file, 'w') as properties:
        properties.write(f'{hosts}\n')
        properties.write('graph.server.port=8182')
        properties.write('graph.client.ssl=false')
        properties.write('graph.client.maxConnectionPoolSize=32')
        properties.write('graph.client.minConnectionPoolSize=32')
        properties.write('graph.client.maxInProcessPerConnection=8')
        properties.write('benchmark.measurementForks=1')
        properties.write('benchmark.measurementIterations=1')
        properties.write('benchmark.measurementTime=100')
        properties.write('benchmark.measurementTimeout=60')
        properties.write('benchmark.measurementThreads=32')
        properties.write('benchmark.mode=all')
        properties.write('benchmark.idBufferSize=5000')

if __name__ == "__main__":
    main(sys.argv)

