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

import os
import subprocess

import pytest

import graph_config.configure_aerospike_graph as config


def _mount_certs(tmp_path, monkeypatch, *, include_ca=True):
    """Create temporary certificate mounts using the real filesystem."""
    keystore_dir = tmp_path / "gremlin-server-tls"
    ca_dir = tmp_path / "gremlin-server-ca"
    ssl_out = tmp_path / "sslout"
    keystore_dir.mkdir()
    ca_dir.mkdir()

    (keystore_dir / "server.crt").write_text("-----BEGIN CERTIFICATE-----\ncertificate")
    (keystore_dir / "server.key").write_text("-----BEGIN PRIVATE KEY-----\nkey")
    if include_ca:
        (ca_dir / "ca.pem").write_text("-----BEGIN CERTIFICATE-----\nca")

    monkeypatch.setattr(config, "GREMLIN_SERVER_TLS_DIR", str(keystore_dir))
    monkeypatch.setattr(config, "GREMLIN_SERVER_CA_DIR", str(ca_dir))

    return keystore_dir, ca_dir, ssl_out


def _capture_openssl(monkeypatch):
    called = {}

    def fake_run(cmd, check):
        called["cmd"] = cmd
        called["check"] = check
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(config.subprocess, "run", fake_run)
    return called


def test_mount_path_constants_are_absolute():
    # Guard against relative certificate mount paths.
    for name in ("GREMLIN_SERVER_TLS_DIR", "GREMLIN_SERVER_CA_DIR", "AEROSPIKE_CLIENT_TLS_DIR"):
        value = getattr(config, name)
        assert value.startswith("/"), f"{name} must be an absolute path, got {value!r}"


def test_generate_server_keystore_calls_openssl(tmp_path, monkeypatch):
    _, _, ssl_out = _mount_certs(tmp_path, monkeypatch)
    called = _capture_openssl(monkeypatch)

    ssl_opts = {"keyStore": str(ssl_out / "keystore.p12"), "keyStorePassword": "aerospike"}
    config.generate_server_keystore(ssl_opts, str(ssl_out))

    assert called["cmd"][0] == "openssl"
    assert "-export" in called["cmd"]


def test_generate_server_keystore_bundles_ca(tmp_path, monkeypatch):
    # A mounted CA must be passed to openssl via -certfile.
    keystore_dir, ca_dir, ssl_out = _mount_certs(tmp_path, monkeypatch, include_ca=True)
    called = _capture_openssl(monkeypatch)

    ssl_opts = {"keyStore": str(ssl_out / "keystore.p12"), "keyStorePassword": "aerospike"}
    config.generate_server_keystore(ssl_opts, str(ssl_out))

    cmd = called["cmd"]
    assert "-certfile" in cmd, "mounted CA was not bundled into the keystore"
    ca_arg = cmd[cmd.index("-certfile") + 1]
    # Compare paths across host platforms.
    assert os.path.basename(ca_arg) == "ca.pem"
    assert os.path.normpath(ca_arg) == os.path.normpath(str(ca_dir / "ca.pem"))


def test_generate_server_keystore_without_ca_omits_certfile(tmp_path, monkeypatch):
    # A missing CA is valid and omits -certfile.
    _, ca_dir, ssl_out = _mount_certs(tmp_path, monkeypatch, include_ca=False)
    ca_dir.rmdir()
    called = _capture_openssl(monkeypatch)

    ssl_opts = {"keyStore": str(ssl_out / "keystore.p12"), "keyStorePassword": "aerospike"}
    config.generate_server_keystore(ssl_opts, str(ssl_out))

    assert called["cmd"][0] == "openssl"
    assert "-certfile" not in called["cmd"]


def test_generate_server_keystore_missing_keystore_dir_raises(tmp_path, monkeypatch):
    # The required certificate mount must fail loudly when absent.
    monkeypatch.setattr(config, "GREMLIN_SERVER_TLS_DIR", str(tmp_path / "does-not-exist"))
    monkeypatch.setattr(config, "GREMLIN_SERVER_CA_DIR", str(tmp_path / "no-ca"))
    _capture_openssl(monkeypatch)

    ssl_opts = {"keyStore": str(tmp_path / "out" / "keystore.p12"), "keyStorePassword": "aerospike"}
    with pytest.raises(Exception):
        config.generate_server_keystore(ssl_opts, str(tmp_path / "out"))
