import pytest
import os
import textwrap
from graph_config.configure_aerospike_graph import generate_yaml

def test_generate_yaml_basic(tmp_path, monkeypatch):
    """Test basic yaml generation"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\nport: 8182\n")
    
    generate_yaml(
        yaml_properties=[],
        default_yaml_file=str(default_yaml),
        output_yaml_file=str(output_yaml),
        graph_config={"graph": []},
        auth_jwt_secret=None,
        auth_jwt_issuer=None,
        auth_jwt_algorithm=None,
        ssl_out_dir=str(tmp_path / "ssl"),
        conf_dir=conf_dir
    )
    
    assert output_yaml.exists()
    content = output_yaml.read_text()
    assert "host: localhost" in content
    assert "port: 8182" in content
    assert "graphs:" in content
    assert "graph:" in content
    assert "metrics:" in content
    assert "ssl:" in content
    assert "serializers:" in content
    assert "processors:" in content


def test_generate_yaml_metrics_configuration(tmp_path, monkeypatch):
    """Test metrics configuration"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\n")
    
    yaml_properties = [
        "aerospike.graph-service.metrics.consoleReporter.enabled=false",
        "aerospike.graph-service.metrics.csvReporter.enabled=true",
        "aerospike.graph-service.metrics.csvReporter.fileName=/tmp/custom.csv",
    ]
    
    generate_yaml(
        yaml_properties=yaml_properties,
        default_yaml_file=str(default_yaml),
        output_yaml_file=str(output_yaml),
        graph_config={"graph": []},
        auth_jwt_secret=None,
        auth_jwt_issuer=None,
        auth_jwt_algorithm=None,
        ssl_out_dir=str(tmp_path / "ssl"),
        conf_dir=conf_dir
    )
    
    content = output_yaml.read_text()
    assert "consoleReporter:" in content
    assert "enabled: false" in content or "enabled:false" in content
    assert "csvReporter:" in content
    assert "/tmp/custom.csv" in content


def test_generate_yaml_metrics_invalid_type(tmp_path, monkeypatch):
    """Test error when invalid metrics type is provided"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\n")

    yaml_properties = [
        "aerospike.graph-service.metrics.invalidReporter.enabled=true",
    ]
    
    with pytest.raises(Exception) as exc:
        generate_yaml(
            yaml_properties=yaml_properties,
            default_yaml_file=str(default_yaml),
            output_yaml_file=str(output_yaml),
            graph_config={"graph": []},
            auth_jwt_secret=None,
            auth_jwt_issuer=None,
            auth_jwt_algorithm=None,
            ssl_out_dir=str(tmp_path / "ssl"),
            conf_dir=conf_dir
        )
    
    assert "invalidReporter" in str(exc.value)
    assert "not a valid metrics type" in str(exc.value)


def test_generate_yaml_ssl_configuration(tmp_path, monkeypatch):
    """Test SSL configuration"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\n")

    yaml_properties = [
        "aerospike.graph-service.ssl.enabled=true",
        "aerospike.graph-service.ssl.keyStore=/custom/keystore.p12",
        "aerospike.graph-service.ssl.keyStorePassword=custompass",
    ]
    
    generate_yaml(
        yaml_properties=yaml_properties,
        default_yaml_file=str(default_yaml),
        output_yaml_file=str(output_yaml),
        graph_config={"graph": []},
        auth_jwt_secret=None,
        auth_jwt_issuer=None,
        auth_jwt_algorithm=None,
        ssl_out_dir=str(tmp_path / "ssl"),
        conf_dir=conf_dir
    )
    
    content = output_yaml.read_text()
    assert "ssl:" in content
    assert "enabled: true" in content or "enabled:true" in content
    assert "/custom/keystore.p12" in content


def test_generate_yaml_ssl_invalid_format(tmp_path, monkeypatch):
    """Test error when SSL config doesnt use ssl prefix"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\n")

    yaml_properties = [
        "aerospike.graph-service.ssl=enabled",
    ]
    
    with pytest.raises(Exception) as exc:
        generate_yaml(
            yaml_properties=yaml_properties,
            default_yaml_file=str(default_yaml),
            output_yaml_file=str(output_yaml),
            graph_config={"graph": []},
            auth_jwt_secret=None,
            auth_jwt_issuer=None,
            auth_jwt_algorithm=None,
            ssl_out_dir=str(tmp_path / "ssl"),
            conf_dir=conf_dir
        )
    
    assert "SSL configurations must specify settings individually" in str(exc.value)


def test_generate_yaml_reserved_keys_error(tmp_path, monkeypatch):
    """Test error when trying to override reserved keys"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\n")

    for reserved_key in ["serializers", "processors", "graphs"]:
        yaml_properties = [
            f"aerospike.graph-service.{reserved_key}=value",
        ]
        
        with pytest.raises(Exception) as exc:
            generate_yaml(
                yaml_properties=yaml_properties,
                default_yaml_file=str(default_yaml),
                output_yaml_file=str(output_yaml),
                graph_config={"graph": []},
                auth_jwt_secret=None,
                auth_jwt_issuer=None,
                auth_jwt_algorithm=None,
                ssl_out_dir=str(tmp_path / "ssl"),
                conf_dir=conf_dir
            )
        
        assert reserved_key in str(exc.value)
        assert "cannot be overwritten" in str(exc.value)


def test_generate_yaml_multiple_graphs(tmp_path, monkeypatch):
    """Test YAML generation with multiple graphs"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\n")
    
    monkeypatch.setattr("graph_config.configure_aerospike_graph.set_performance_mode", lambda x: None)
    monkeypatch.setattr("graph_config.configure_aerospike_graph.find_security_credentials", lambda *args: None)
    
    generate_yaml(
        yaml_properties=[],
        default_yaml_file=str(default_yaml),
        output_yaml_file=str(output_yaml),
        graph_config={"graph": [], "graph2": []},
        auth_jwt_secret=None,
        auth_jwt_issuer=None,
        auth_jwt_algorithm=None,
        ssl_out_dir=str(tmp_path / "ssl"),
        conf_dir=conf_dir
    )
    
    content = output_yaml.read_text()
    assert "graph:" in content
    assert "graph2:" in content
    assert "aerospike-graph-graph.properties" in content
    assert "aerospike-graph-graph2.properties" in content


def test_generate_yaml_custom_property(tmp_path, monkeypatch):
    """Test custom yaml property"""
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "output.yaml"
    conf_dir = str(tmp_path / "conf")
    os.makedirs(conf_dir, exist_ok=True)
    
    default_yaml.write_text("host: localhost\ncustomKey: oldValue\n")
    
    yaml_properties = [
        "aerospike.graph-service.customKey=newValue",
    ]
    
    generate_yaml(
        yaml_properties=yaml_properties,
        default_yaml_file=str(default_yaml),
        output_yaml_file=str(output_yaml),
        graph_config={"graph": []},
        auth_jwt_secret=None,
        auth_jwt_issuer=None,
        auth_jwt_algorithm=None,
        ssl_out_dir=str(tmp_path / "ssl"),
        conf_dir=conf_dir
    )
    
    content = output_yaml.read_text()
    # Old value should be replaced
    assert "customKey: newValue" in content
    assert "customKey: oldValue" not in content

