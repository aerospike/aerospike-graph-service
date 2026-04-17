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

from graph_config.configure_aerospike_graph import generate_server_keystore
import subprocess
import os

def test_generate_server_keystore_calls_openssl(tmp_path, monkeypatch):
    # Set up fake directories and files
    keystore_dir = tmp_path / "gremlin-server-tls"
    ca_dir = tmp_path / "gremlin-server-ca"
    ssl_out = tmp_path / "sslout"
    keystore_dir.mkdir()
    ca_dir.mkdir()

    cert = keystore_dir / "server.crt"
    key = keystore_dir / "server.key"
    ca = ca_dir / "ca.pem"

    cert.write_text("-----BEGIN CERTIFICATE-----\ncertificate")
    key.write_text("-----BEGIN PRIVATE KEY-----\nkey")
    ca.write_text("-----BEGIN CERTIFICATE-----\nca")

    # Function to mock os.path.isdir to return True for the hardcoded paths
    original_isdir = os.path.isdir
    def mock_isdir(path):
        if path == "/opt/aerospike-graph/gremlin-server-tls":
            return True
        elif path == "opt/aerospike-graph/gremlin-server-ca":
            return True
        return original_isdir(path)

    # Function to mock os.listdir so that it checks our configured paths only
    def mock_listdir(path):
        if isinstance(path, bytes):
            if path == os.fsencode("/opt/aerospike-graph/gremlin-server-tls"):
                return [os.fsencode("server.crt"), os.fsencode("server.key")]
            elif path == os.fsencode("opt/aerospike-graph/gremlin-server-ca"):
                return [os.fsencode("ca.pem")]
        return os.listdir(path)
    
    # Function to mock open to read from our test files
    original_open = open
    def mock_open(path, mode='r', *args, **kwargs):
        if path == "/opt/aerospike-graph/gremlin-server-tls/server.crt":
            return original_open(str(cert), mode, *args, **kwargs)
        elif path == "/opt/aerospike-graph/gremlin-server-tls/server.key":
            return original_open(str(key), mode, *args, **kwargs)
        elif path == "opt/aerospike-graph/gremlin-server-ca/ca.pem":
            return original_open(str(ca), mode, *args, **kwargs)
        return original_open(path, mode, *args, **kwargs)

    # Mock Functions
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", mock_isdir)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.listdir", mock_listdir)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.open", mock_open, raising=False)
    monkeypatch.setattr("builtins.open", mock_open, raising=False)

    called = {}

    def fake_run(cmd, check):
        called["cmd"] = cmd
        called["check"] = check
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.subprocess.run", fake_run)

    ssl_opts = {
        "keyStore": str(ssl_out / "keystore.p12"),
        "keyStorePassword": "aerospike"
    }

    generate_server_keystore(ssl_opts, str(ssl_out))

    assert "cmd" in called
    assert called["cmd"][0] == "openssl"
    assert "-export" in called["cmd"]
