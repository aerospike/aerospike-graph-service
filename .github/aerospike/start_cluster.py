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

import logging
import os.path
import tempfile
import time
from typing import List, Dict

import argparse
import docker
import jinja2
from box import Box
from docker.client import DockerClient


def parse_cluster_cli():
    parser = argparse.ArgumentParser(description="run aerospike cluster")
    parser.add_argument('--aerospike_version', type=str, default="8.1", help="version of aerospike")
    parser.add_argument('--config_template', type=str, default="aerospike_base.conf.j2",
                        help="jinja config template name")
    parser.add_argument('--features_file', type=str, help="base64 encoded features file")
    parser.add_argument('--repo_path', type=str, help="repo path")
    parser.add_argument('--node_count', help="number of nodes", type=int, default=3)
    parser.add_argument('--debug', help="debug logging", action="store_true", default=False)
    parser.add_argument('--test', help="test", action="store_true", default=False)
    parser.add_argument('--default_ttl', help="Default ttl for Aerospike nodes", default=0, type=str)
    parser.add_argument('--sc', help="enable strong consistency mode", action="store_true", default=False)

    cli = parser.parse_args()
    assert cli.repo_path is not None
    assert cli.aerospike_version is not None
    assert cli.features_file is not None

    return Box({"aerospike_version": cli.aerospike_version,
                "config_template": cli.config_template,
                "features_file": cli.features_file,
                "repo_path": cli.repo_path,
                "aerospike_image": f"aerospike/aerospike-server-enterprise:{cli.aerospike_version}",
                "node_count": cli.node_count,
                "debug": cli.debug,
                "default_ttl": cli.default_ttl,
                "sc": cli.sc if "sc" in cli else False,
                "test": cli.test if "test" in cli else False})


def get_mounts(config, tempdir) -> List[Dict]:
    return [{
        "Target": "/opt/aerospike/etc",
        "Mode": "",
        "Propagation": "rprivate",
        "RW": True,
        "Source": f"{config.repo_path}/.github/aerospike",
        "Type": "bind"
    }, {
        "Target": "/opt/aerospike/etc/rendered_configs",
        "Mode": "",
        "Propagation": "rprivate",
        "RW": True,
        "Source": tempdir,
        "Type": "bind"
    }]


class ClusterManager:
    def __init__(self, config: Box):
        self.config = config
        logging.basicConfig(level=logging.DEBUG if config.debug else logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

    def start_aerospike_node(self, numeric_id: int, config: Box, docker_client: DockerClient):
        temp_dir = tempfile.mkdtemp("aerospike_config")
        mounts = get_mounts(config, temp_dir)

        if config.aerospike_version.startswith("6."):
            legacy_memory_setting = "memory-size 3G"
        else:
            legacy_memory_setting = ""
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(os.path.realpath(__file__)))
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(config.config_template)

        template_output = template.render(
            legacy_memory_setting=legacy_memory_setting,
            access_port=f"30{numeric_id}0",
            port=f"30{numeric_id}0",
            tls_port=f"43{numeric_id}3",
            default_ttl=config.default_ttl
        )
        node_config_file = os.path.join(temp_dir, f"aerospike_{numeric_id}.conf")
        with open(node_config_file, "w") as fh:
            fh.write(template_output)
            fh.close()

        it = docker_client.containers.run(config.aerospike_image,
                                          command=f" --config-file /opt/aerospike/etc/rendered_configs/aerospike_{numeric_id}.conf",
                                          environment={"FEATURE_KEY_FILE": "/opt/aerospike/etc/features.conf"},
                                          ports={f'30{numeric_id}0/tcp': f'30{numeric_id}0/tcp',
                                                 f'43{numeric_id}3/tcp': f'43{numeric_id}3/tcp'},
                                          detach=True,
                                          mounts=mounts)
        self.logger.info(f"started aerospike container {it.id}")
        self.logger.debug(f"rendered config: \n {template_output}")
        return it.id

    def run_asinfo_cmd(self, cmd: str, ctr_id: str, docker_client: DockerClient):
        self.logger.info(f"{ctr_id}\n\tcmd: {cmd}\n\t")
        container = docker_client.containers.get(ctr_id)
        exit, output = container.exec_run(cmd)
        if exit != 0:
            self.logger.error(f"asinfo error: {output}")
        self.logger.info(output)
        return exit, output

    def run_asinfo_tip(self, port, ctr_id: str, peer_ip: str, docker_client: DockerClient, retries: int = 5, delay: float = 2.0):
        """Run tip command with retry logic to handle slow Aerospike startup."""
        cmd = f"asinfo -p {port} -v tip:host={peer_ip};port=3002"
        for attempt in range(retries):
            exit_code, output = self.run_asinfo_cmd(cmd, ctr_id, docker_client)
            if exit_code == 0:
                return exit_code, output
            if attempt < retries - 1:
                self.logger.info(f"Tip command failed, retrying in {delay}s (attempt {attempt + 1}/{retries})")
                time.sleep(delay)
        return exit_code, output

    def get_ctr_ip(self, ctr_id: str, docker_client: DockerClient) -> str:
        container = docker_client.containers.get(ctr_id)
        container.reload()  # Ensure we have the latest attrs
        network_settings = container.attrs['NetworkSettings']
        
        # Try the direct IPAddress first (default bridge network)
        x = network_settings.get('IPAddress', '')
        
        # If empty, check in the Networks dict (user-defined networks or newer Docker)
        if not x and 'Networks' in network_settings:
            networks = network_settings['Networks']
            for network_name, network_info in networks.items():
                if network_info.get('IPAddress'):
                    x = network_info['IPAddress']
                    break
        
        self.logger.info(f"container {ctr_id} ip: {x}")
        return x

    def cluster_healthcheck(self, port, ctr_id: str, docker_client: DockerClient, cluster_size) -> bool:
        self.logger.info(f"will healthcheck {ctr_id} with port {port}")
        cmd = f"asinfo -p {port} -v cluster-stable:size={cluster_size};ignore-migrations=true;namespace=test"
        self.logger.debug(cmd)

        exit, output = self.run_asinfo_cmd(cmd, ctr_id, docker_client)
        self.logger.info(f"healthcheck: {output}")
        return exit == 0

    def cluster_sc(self, nodes: list, docker_client: DockerClient, cluster_size) -> bool:
        # get list of available nodes
        cmd = f"asinfo -p 3000 -v roster:namespace=test"
        exit, output = self.run_asinfo_cmd(cmd, nodes[0], docker_client)
        s = str(output)
        idx = s.index('nodes=')

        # It is safe to issue this command multiple times, once for each observed node.
        for i in range(0, cluster_size):
            cmd = f"asinfo -p 30{i}0 -v roster-set:namespace=test;{s[idx:-3]}"
            self.run_asinfo_cmd(cmd, nodes[i], docker_client)

        # only principal node is able to handle this request, other nodes will ignore it.
        for i in range(0, cluster_size):
            cmd = f"asinfo -p 30{i}0 -v recluster:"
            self.run_asinfo_cmd(cmd, nodes[i], docker_client)

        time.sleep(5) # seconds

        # verify recluster
        cmd = f"asinfo -p 3000 -v roster:namespace=test"
        exit, output = self.run_asinfo_cmd(cmd, nodes[0], docker_client)
        self.logger.info("roster: " + str(output))

        return exit == 0

    def single_healthcheck(self, port, ctr_id: str, docker_client: DockerClient) -> bool:
        self.logger.info(f"will healthcheck {ctr_id} with port {port}")
        cmd = f"asinfo -p {port} -v namespaces"
        exit, output = self.run_asinfo_cmd(cmd, ctr_id, docker_client)
        self.logger.info(f"healthcheck: {output}")
        return exit == 0

    def shutdown(self, ctr_id: str, docker_client: DockerClient):
        container = docker_client.containers.get(ctr_id)
        self.logger.info(f"stopping container {ctr_id} ...")
        container.stop()

    def start_aerospike_single(self, config, docker_client):
        image = config.aerospike_image.split(':')[0]
        tag = config.aerospike_image.split(':')[1]
        docker_client.images.pull(repository=image, tag=tag)
        ctr_id: str = self.start_aerospike_node(0, config, docker_client)
        self.logger.info(f"started aerospike container {ctr_id}")
        time.sleep(2)
        self.single_healthcheck(3000, ctr_id, docker_client)
        return ctr_id

    def start_aerospike_cluster(self, config, docker_client, node_count):
        image = config.aerospike_image.split(':')[0]
        tag = config.aerospike_image.split(':')[1]
        docker_client.images.pull(repository=image, tag=tag)
        nodes: List = []
        for i in range(node_count):
            container_id = self.start_aerospike_node(i, config, docker_client)
            self.logger.info(f"started aerospike container {i} {container_id} {self.get_ctr_ip(container_id, docker_client)}")
            nodes.append(container_id)
        
        # Wait for Aerospike to initialize inside containers before running tip commands
        self.logger.info("Waiting for Aerospike to initialize in containers...")
        time.sleep(5)
        
        for i in range(node_count):
            self.logger.info(f"tip container {nodes[i]} to peer with first container {nodes[0]}")
            self.run_asinfo_tip(f"30{i}0", nodes[i], self.get_ctr_ip(nodes[0], docker_client), docker_client)
        return nodes


if __name__ == '__main__':
    docker_client: DockerClient = docker.from_env()
    config = parse_cluster_cli()
    cluster_manager = ClusterManager(config)

    if config.node_count == 1:
        if config.sc:
            raise Exception("SC mode is not supported for single node cluster")
        ctr_id = cluster_manager.start_aerospike_single(config, docker_client)
    elif config.node_count > 9:
        raise Exception("more than 9 nodes not supported")
    else:
        nodes: List = cluster_manager.start_aerospike_cluster(config, docker_client, config.node_count)
        time.sleep(3)
        for i in range(0, config.node_count - 1):
            assert cluster_manager.cluster_healthcheck(f"30{i}0", nodes[i], docker_client, config.node_count)
        if config.sc:
            cluster_manager.cluster_sc(nodes, docker_client, config.node_count)

        if "test" in config and config.test:
            for node in nodes:
                cluster_manager.shutdown(node, docker_client)
