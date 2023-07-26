import json
import sys


def main(argv):
    github_sha = argv[1]
    hosts = 'aerospike.client.host='

    with open('./clusters.json') as file:
        clusters = json.load(file)

        for cluster in clusters:
            if github_sha in cluster['ClusterName']:
                hosts = hosts + cluster['PrivateIp'] + ':3000,'
        hosts = hosts[:-1]
        with open('l3_config_base.properties', 'r') as base_properties:
            with open("l3_config.properties", 'w') as properties:
                properties.write(f'{hosts}\n')
                properties.write(base_properties.read())


if __name__ == "__main__":
    main(sys.argv)

