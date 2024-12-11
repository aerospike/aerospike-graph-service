import json
import sys


def main(argv):
    aerospike_graph_name = argv[1]
    benchmark_config_file = argv[2]
    hosts = 'graph.server.host='
    ip_only = ''
    with open('./clients.json') as file:
        clients = json.load(file)

        for client in clients:
            if aerospike_graph_name in client['ClientName']:
                ip_only = ip_only + client['PrivateIp']
                print(f"Found {aerospike_graph_name} at {ip_only}")
        ip_only = ip_only[:-1]
        hosts = hosts + ip_only

    if hosts == 'graph.server.host=':
        print('Client not found')
        sys.exit(1)

    with open(benchmark_config_file, 'w') as properties:
        properties.write(f'{hosts}\n')
        properties.write('graph.server.port=8182\n')
        properties.write('graph.client.ssl=false\n')
        properties.write('graph.client.maxConnectionPoolSize=32\n')
        properties.write('graph.client.minConnectionPoolSize=32\n')
        properties.write('graph.client.maxInProcessPerConnection=8\n')
        properties.write('benchmark.measurementForks=1\n')
        properties.write('benchmark.measurementIterations=1\n')
        properties.write('benchmark.measurementTime=100\n')
        properties.write('benchmark.measurementTimeout=60\n')
        properties.write('benchmark.measurementThreads=32\n')
        properties.write('benchmark.mode=all\n')
        properties.write('benchmark.idBufferSize=5000\n')

if __name__ == "__main__":
    main(sys.argv)

