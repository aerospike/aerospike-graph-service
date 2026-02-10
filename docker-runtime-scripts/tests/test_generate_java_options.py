import pytest
import os
import subprocess
from graph_config.configure_aerospike_graph import generate_java_options


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
    """Test TLS truststore generation when cert directory exists"""
    output_file = tmp_path / "java_options.txt"
    tls_dir = tmp_path / "tls"
    cert_dir = tmp_path / "aerospike-client-tls"
    cert_dir.mkdir()
    
    # Fake a cert file
    cert_file = cert_dir / "ca.crt"
    cert_file.write_text("-----BEGIN CERTIFICATE-----\nfake cert\n")
    
    # Function to mock os.path.isdir to return True for cert_dir
    original_isdir = os.path.isdir
    def mock_isdir(path):
        if path == "/opt/aerospike-graph/aerospike-client-tls":
            return True
        return original_isdir(path)
    
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.path.isdir", mock_isdir)
    
    # Function to mock os.listdir to return cert file we made
    def mock_listdir(path):
        if path == os.fsencode("/opt/aerospike-graph/aerospike-client-tls"):
            return [os.fsencode("ca.crt")]
        return os.listdir(path)
    
    monkeypatch.setattr("graph_config.configure_aerospike_graph.os.listdir", mock_listdir)
    
    # Function to mock subprocess.run for keytool
    called = {}
    def fake_run(cmd, check):
        called["cmd"] = cmd
        called["check"] = check
        return subprocess.CompletedProcess(cmd, 0)

    # Mock functions
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
    
    # Verify keytool was called
    assert "cmd" in called
    assert called["cmd"][0] == "keytool"
    assert "-import" in called["cmd"]


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