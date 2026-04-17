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

import pytest
import os
import subprocess
from graph_config.configure_aerospike_graph import generate_java_options, _find_system_cacerts


def test_generate_java_options_with_heaps(tmp_path):
    """Test Java options generation with both min and max heap"""
    output_file = tmp_path / "java_options.txt"
    
    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap="aerospike.graph-service.heap.max=2048m",
        min_heap="aerospike.graph-service.heap.min=1024m",
        tls_out_dir=str(tmp_path / "tls")
    )
    
    assert output_file.exists()
    content = output_file.read_text()
    assert "-Xmx2048m" in content
    assert "-Xms1024m" in content


def test_generate_java_options_default_uses_max_ram_percentage(tmp_path):
    """Test that when no heap is specified, MaxRAMPercentage is used instead of a fixed -Xmx"""
    output_file = tmp_path / "java_options.txt"
    
    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap=None,
        min_heap=None,
        tls_out_dir=str(tmp_path / "tls")
    )
    
    assert output_file.exists()
    content = output_file.read_text()
    assert "-XX:MaxRAMPercentage=80.0" in content
    assert "-Xmx" not in content


def test_generate_java_options_with_java_options_env(tmp_path, monkeypatch):
    """Test that JAVA_OPTIONS environment variable is appended"""
    output_file = tmp_path / "java_options.txt"
    
    monkeypatch.setenv("JAVA_OPTIONS", "-Dcustom.property=value")
    
    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap="aerospike.graph-service.heap.max=512m",
        min_heap=None,
        tls_out_dir=str(tmp_path / "tls")
    )
    
    assert output_file.exists()
    content = output_file.read_text()
    assert "-Xmx512m" in content
    assert "-Dcustom.property=value" in content


def test_generate_java_options_password_masking(tmp_path, monkeypatch, capsys):
    """Test that passwords in JAVA_OPTIONS are masked in print output"""
    output_file = tmp_path / "java_options.txt"
    
    monkeypatch.setenv("JAVA_OPTIONS", "-Dpassword=secret123 -Dother=value")
    
    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap=None,
        min_heap=None,
        tls_out_dir=str(tmp_path / "tls")
    )
    
    # Check that password is masked in printed output
    captured = capsys.readouterr()
    assert "password=***" in captured.out or "password=********" in captured.out or "*" in captured.out
    
    # Check file contains real password
    content = output_file.read_text()
    assert "password=secret123" in content


def test_generate_java_options_tls_truststore(tmp_path, monkeypatch):
    """Test TLS truststore generation when cert directory exists.

    Verifies that:
    - the truststore is seeded from system cacerts via keytool -importkeystore
    - the Aerospike CA cert is imported via keytool -import
    - javax.net.ssl JVM options are written to the output file
    """
    output_file = tmp_path / "java_options.txt"
    tls_dir = tmp_path / "tls"
    cert_dir = tmp_path / "aerospike-client-tls"
    cert_dir.mkdir()

    fake_cacerts = tmp_path / "cacerts"
    fake_cacerts.write_bytes(b"fake-cacerts-content")

    cert_file = cert_dir / "ca.crt"
    cert_file.write_text("-----BEGIN CERTIFICATE-----\nfake cert\n")

    original_isdir = os.path.isdir
    def mock_isdir(path):
        if path == "/opt/aerospike-graph/aerospike-client-tls":
            return True
        return original_isdir(path)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", mock_isdir)

    def mock_listdir(path):
        if path == os.fsencode("/opt/aerospike-graph/aerospike-client-tls"):
            return [os.fsencode("ca.crt")]
        return os.listdir(path)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.listdir", mock_listdir)
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph._find_system_cacerts",
        lambda: str(fake_cacerts)
    )

    calls = []
    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.subprocess.run", fake_run)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.exists", lambda p: False)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.makedirs", lambda p, **kw: None)

    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap="aerospike.graph-service.heap.max=512m",
        min_heap=None,
        tls_out_dir=str(tls_dir)
    )

    assert output_file.exists()
    content = output_file.read_text()
    assert "-Djavax.net.ssl.trustStore" in content
    assert "truststore.jks" in content
    assert "-Djavax.net.ssl.trustStorePassword=aerospike" in content

    # First call: keytool -importkeystore to seed truststore from system CAs
    assert len(calls) >= 2
    assert calls[0][0] == "keytool"
    assert "-importkeystore" in calls[0]
    assert "-srcstorepass" in calls[0]
    assert "changeit" in calls[0]
    assert "-deststorepass" in calls[0]
    assert "aerospike" in calls[0]

    # Second call: keytool -import for the Aerospike CA cert
    assert calls[1][0] == "keytool"
    assert "-import" in calls[1]
    assert "ca.crt" in " ".join(calls[1])


def test_generate_java_options_tls_truststore_no_system_cacerts(tmp_path, monkeypatch):
    """Test TLS truststore generation when system cacerts cannot be found.

    The truststore should still be created and the Aerospike CA imported;
    a warning is printed but no exception is raised.
    """
    output_file = tmp_path / "java_options.txt"
    tls_dir = tmp_path / "tls"

    original_isdir = os.path.isdir
    def mock_isdir(path):
        if path == "/opt/aerospike-graph/aerospike-client-tls":
            return True
        return original_isdir(path)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", mock_isdir)

    def mock_listdir(path):
        if path == os.fsencode("/opt/aerospike-graph/aerospike-client-tls"):
            return [os.fsencode("ca.crt")]
        return os.listdir(path)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.listdir", mock_listdir)
    monkeypatch.setattr("graph_config.configure_aerospike_graph._find_system_cacerts", lambda: None)

    calls = []
    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("graph_config.configure_aerospike_graph.subprocess.run", fake_run)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.exists", lambda p: False)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.makedirs", lambda p, **kw: None)

    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap="aerospike.graph-service.heap.max=512m",
        min_heap=None,
        tls_out_dir=str(tls_dir)
    )

    content = output_file.read_text()
    assert "-Djavax.net.ssl.trustStore" in content

    # Only the keytool -import call; no -importkeystore since cacerts was missing
    assert len(calls) == 1
    assert calls[0][0] == "keytool"
    assert "-import" in calls[0]


def test_find_system_cacerts_alpine_primary(monkeypatch):
    """Test that _find_system_cacerts finds Alpine's /etc/ssl/certs/java/cacerts first."""
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph.os.path.isfile",
        lambda p: p == "/etc/ssl/certs/java/cacerts",
    )
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", lambda p: False)

    assert _find_system_cacerts() == "/etc/ssl/certs/java/cacerts"


def test_find_system_cacerts_alpine_jvm_symlink(monkeypatch):
    """Test fallback to /usr/lib/jvm/java-17-openjdk/lib/security/cacerts."""
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph.os.path.isfile",
        lambda p: p == "/usr/lib/jvm/java-17-openjdk/lib/security/cacerts",
    )
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", lambda p: False)

    assert _find_system_cacerts() == "/usr/lib/jvm/java-17-openjdk/lib/security/cacerts"


def test_find_system_cacerts_via_java_home(tmp_path, monkeypatch):
    """Test discovery via JAVA_HOME on a non-Alpine distro."""
    java_home = tmp_path / "jdk-17"
    cacerts = java_home / "lib" / "security" / "cacerts"
    cacerts.parent.mkdir(parents=True)
    cacerts.write_bytes(b"fake")

    monkeypatch.setenv("JAVA_HOME", str(java_home))
    # Alpine paths don't exist
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph.os.path.isfile",
        lambda p: os.path.exists(p),
    )
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", lambda p: os.path.exists(p))

    assert _find_system_cacerts() == str(cacerts)


def test_find_system_cacerts_walk_fallback(tmp_path, monkeypatch):
    """Test os.walk fallback when neither Alpine nor JAVA_HOME paths exist."""
    jvm_root = tmp_path / "usr" / "lib" / "jvm" / "java-17-amazon" / "lib" / "security"
    jvm_root.mkdir(parents=True)
    (jvm_root / "cacerts").write_bytes(b"fake")

    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph.os.path.isfile",
        lambda p: os.path.exists(p),
    )
    original_isdir = os.path.isdir
    def mock_isdir(path):
        if path == "/usr/lib/jvm":
            return True
        if path in ("/etc/pki/java", "/etc/ssl/certs/java"):
            return False
        return original_isdir(path)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", mock_isdir)

    real_walk = os.walk
    redirect_target = str(tmp_path / "usr" / "lib" / "jvm")
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph.os.walk",
        lambda root: real_walk(redirect_target),
    )

    result = _find_system_cacerts()
    assert result is not None
    assert result.endswith("cacerts")


def test_find_system_cacerts_returns_none_when_not_found(monkeypatch):
    """Test that _find_system_cacerts returns None when no cacerts can be found."""
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isfile", lambda p: False)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", lambda p: False)

    assert _find_system_cacerts() is None


def test_generate_java_options_no_tls_when_dir_missing(tmp_path, monkeypatch):
    """Test that TLS truststore is not generated when cert directory doesn't exist"""
    output_file = tmp_path / "java_options.txt"
    tls_dir = tmp_path / "tls"
    
    # Function to mock os.path.isdir to return False for is_dir certification
    original_isdir = os.path.isdir
    def mock_isdir(path):
        if path == "/opt/aerospike-graph/aerospike-client-tls":
            return False
        return original_isdir(path)
    
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", mock_isdir)
    
    generate_java_options(
        java_options_file_path=str(output_file),
        max_heap="aerospike.graph-service.heap.max=512m",
        min_heap=None,
        tls_out_dir=str(tls_dir)
    )
    
    assert output_file.exists()
    content = output_file.read_text()
    # Should not have truststore options
    assert "-Djavax.net.ssl.trustStore" not in content
