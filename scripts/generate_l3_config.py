import json
import sys


def main(argv):
    github_sha = argv[1]
    hosts = 'aerospike.client.host='
    ip_only = ''

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
                properties.write(base_properties.read())
        with open('hosts.txt', 'x') as host_txt:
            host_txt.write(ip_only)


if __name__ == "__main__":
    main(sys.argv)

