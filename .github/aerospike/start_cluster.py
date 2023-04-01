import time
from typing import List, Dict

import argparse
import docker
import logging as logger
from box import Box
from docker.client import DockerClient


def parse_cli():
    parser = argparse.ArgumentParser(description="run aerospike cluster")
    parser.add_argument('--aerospike_version', type=str, help="version of aerospike")
    parser.add_argument('--features_file', type=str, help="base64 encoded features file")
    parser.add_argument('--repo_path', type=str, help="repo path")
    parser.add_argument('--debug', help="debug logging", action="store_true", default=False)
    parser.add_argument('--test', help="test", action="store_true", default=False)

    cli = parser.parse_args()
    assert cli.repo_path is not None
    assert cli.aerospike_version is not None
    assert cli.features_file is not None

    return Box({"aerospike_version": cli.aerospike_version,
                "features_file": cli.features_file,
                "repo_path": cli.repo_path,
                "aerospike_image": f"aerospike:{cli.aerospike_version}",
                "debug": cli.debug,
                "test": cli.test if "test" in cli else False})


def get_mounts(config) -> List[Dict]:
    return [{
        "Target": "/opt/aerospike/etc",
        "Mode": "",
        "Propagation": "rprivate",
        "RW": True,
        "Source": f"{config.repo_path}/.github/aerospike",
        "Type": "bind"
    }]


def start_aerospike_node(incr: int, config: Box, docker_client: DockerClient):
    mounts = get_mounts(config)

    it = docker_client.containers.run(config.aerospike_image,
                                      command=f" --config-file /opt/aerospike/etc/aerospike_{incr}.conf",
                                      environment={"FEATURE_KEY_FILE": "/opt/aerospike/etc/features.conf",
                                                   "MEM_GB": 2},
                                      ports={f'30{incr}0/tcp': f'30{incr}0/tcp',
                                             f'43{incr}3/tcp': f'43{incr}3/tcp'},
                                      detach=True,
                                      mounts=mounts)
    logger.info(f"started aerospike container {it.id}")
    return it.id


def run_asinfo_cmd(cmd: str, ctr_id: str, docker_client: DockerClient):
    container = docker_client.containers.get(ctr_id)
    exit, output = container.exec_run(cmd)
    if exit != 0:
        logger.error(f"asinfo error: {output}")
    logger.info(output)
    return exit, output


def run_asinfo_tip(incr: int, ctr_id: str, peer_ip: str, docker_client: DockerClient):
    return run_asinfo_cmd(f"asinfo -p 30{incr}0 -v tip:host={peer_ip};port=3002", ctr_id, docker_client)


def get_ctr_ip(ctr_id: str, docker_client: DockerClient) -> str:
    container = docker_client.containers.get(ctr_id)
    x = container.attrs['NetworkSettings']['IPAddress']
    logger.info(f"container {ctr_id} ip: {x}")
    return x


def healthcheck(incr, ctr_id: str, docker_client: DockerClient) -> bool:
    logger.info(f"will healthcheck {ctr_id} with incr {incr}")
    cmd = f"asinfo -p 30{incr}0 -v cluster-stable:size=3;ignore-migrations=true;namespace=test"
    exit, output = run_asinfo_cmd(cmd, ctr_id, docker_client)
    logger.info(f"healthcheck: {output}")
    return exit == 0


def shutdown(ctr_id: str, docker_client: DockerClient):
    container = docker_client.containers.get(ctr_id)
    logger.info(f"stopping container {ctr_id} ...")
    container.stop()


def start_aerospike_cluster(config, docker_client):
    ctr_id_0: str = start_aerospike_node(0, config, docker_client)
    logger.info(f"started aerospike container {ctr_id_0}")
    ctr_id_1: str = start_aerospike_node(1, config, docker_client)
    logger.info(f"started aerospike container {ctr_id_1}")
    ctr_id_2: str = start_aerospike_node(2, config, docker_client)
    logger.info(f"started aerospike container {ctr_id_2}")

    # tip container 2 to peer with container 1
    logger.info(f"tip container {ctr_id_1} to peer with container {ctr_id_0}")
    run_asinfo_tip(1, ctr_id_1, get_ctr_ip(ctr_id_0, docker_client), docker_client)
    # tip container 3 to peer with container 1
    logger.info(f"tip container {ctr_id_2} to peer with container {ctr_id_0}")
    run_asinfo_tip(2, ctr_id_2, get_ctr_ip(ctr_id_0, docker_client), docker_client)

    return [ctr_id_0, ctr_id_1, ctr_id_2]


if __name__ == '__main__':
    docker_client: DockerClient = docker.from_env()
    config = parse_cli()
    logger.basicConfig(level=logger.INFO)
    if (config.debug):
        logger.basicConfig(level=logger.DEBUG)
    nodes: List = start_aerospike_cluster(config, docker_client)
    time.sleep(2)
    for i in range(0, 3):
        assert healthcheck(i, nodes[i], docker_client)
    if "test" in config and config.test:
        for node in nodes:
            shutdown(node, docker_client)
