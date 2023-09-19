import json
import sys


def main(argv):
    github_sha = argv[1]
    dataset_size = argv[2]
    hosts = 'aerospike.client.host='
    ip_only = ''
    bucket_root = 'gs://incremental-datasets/' + dataset_size + '/'
    vertices_path = 'aerospike.graphloader.vertices=' + bucket_root + 'vertices'
    edges_path = 'aerospike.graphloader.edges=' + bucket_root + 'edges'
    edgeid_path = 'aerospike.graphloader.edgeid=' + bucket_root +  github_sha +'/'+'edgeid'

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
                properties.write(f'{edgeid_path}\n')
                properties.write(base_properties.read())
        with open('hosts.txt', 'x') as host_txt:
            host_txt.write(ip_only)


if __name__ == "__main__":
    main(sys.argv)

