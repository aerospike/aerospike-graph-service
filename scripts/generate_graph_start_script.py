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
        properties.write("echo {ghcr_io_login} | sudo docker login ghcr.io -u aerobot-firefly --password-stdin\n")
        properties.write(f"sudo docker run --name firefly -d -p 8182:8182 -p 9090:9090 -v $(pwd)/graph-config.properties:/opt/aerospike-graph/aerospike-graph.properties {ags_image}\n")

if __name__ == "__main__":
    main(sys.argv)

