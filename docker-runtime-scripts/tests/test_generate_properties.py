import pytest
from graph_config.configure_aerospike_graph import generate_properties


def test_generate_properties_basic(tmp_path):
    """Test basic properties file generation"""
    output_file = tmp_path / "test.properties"
    
    properties = [
        "aerospike.namespace=test",
        "aerospike.hosts=localhost:3000",
    ]
    
    generate_properties(
        properties=properties,
        output_properties_file=str(output_file),
        auth_jwt_secret=None,
        auth_jwt_issuer=None
    )
    
    assert output_file.exists()
    content = output_file.read_text()
    assert "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph" in content
    assert "aerospike.namespace=test" in content
    assert "aerospike.hosts=localhost:3000" in content


def test_generate_properties_with_gremlin_graph(tmp_path):
    """Test that gremlin.graph is not duplicated if already present"""
    output_file = tmp_path / "test.properties"
    
    properties = [
        "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph",
        "aerospike.namespace=test",
    ]
    
    generate_properties(
        properties=properties,
        output_properties_file=str(output_file),
        auth_jwt_secret=None,
        auth_jwt_issuer=None
    )
    
    content = output_file.read_text()
    # Should only appear once
    assert content.count("gremlin.graph=com.aerospike.firefly.structure.FireflyGraph") == 1


def test_generate_properties_deduplication(tmp_path):
    """Test that duplicate properties are deduplicated and the last one is kept"""
    output_file = tmp_path / "test.properties"
    
    properties = [
        "aerospike.namespace=first",
        "aerospike.hosts=localhost:3000",
        "aerospike.namespace=second",
        "aerospike.namespace=third",   # Last duplicate, should be kept
    ]
    
    generate_properties(
        properties=properties,
        output_properties_file=str(output_file),
        auth_jwt_secret=None,
        auth_jwt_issuer=None
    )
    
    content = output_file.read_text()
    lines = content.split('\n')
    
    # Count occurrences
    namespace_count = content.count("aerospike.namespace=")

    assert namespace_count == 1
    # Should be the last value included
    assert "aerospike.namespace=third" in content


def test_generate_properties_ordering(tmp_path):
    """Test that properties are written in reverse order (last in list appears first)"""
    output_file = tmp_path / "test.properties"
    
    properties = [
        "aerospike.first=1",
        "aerospike.second=2",
        "aerospike.third=3",
    ]
    
    generate_properties(
        properties=properties,
        output_properties_file=str(output_file),
        auth_jwt_secret=None,
        auth_jwt_issuer=None
    )
    
    content = output_file.read_text()
    lines = [line.strip() for line in content.split('\n') if line.strip() and not line.startswith('#')]
    
    # Find positions
    first_pos = next(i for i, line in enumerate(lines) if "aerospike.first=1" in line)
    second_pos = next(i for i, line in enumerate(lines) if "aerospike.second=2" in line)
    third_pos = next(i for i, line in enumerate(lines) if "aerospike.third=3" in line)
    
    # Due to reversed iteration, third should come before second, which should come before first
    # (excluding gremlin.graph which is added first)
    gremlin_pos = next(i for i, line in enumerate(lines) if "gremlin.graph" in line)
    
    # All aerospike properties should come after gremlin.graph
    assert gremlin_pos < first_pos
    assert gremlin_pos < second_pos
    assert gremlin_pos < third_pos
    
    # Properties should be in reverse order (third, second, first)
    assert third_pos < second_pos < first_pos


def test_generate_properties_auth_enabled_with_secret_and_issuer(tmp_path):
    """Test that auth.enabled is set when both secret and issuer are provided"""
    output_file = tmp_path / "test.properties"
    
    properties = [
        "aerospike.namespace=test",
    ]
    
    generate_properties(
        properties=properties,
        output_properties_file=str(output_file),
        auth_jwt_secret="aerospike.graph-service.auth.jwt.secret=mysecret",
        auth_jwt_issuer="aerospike.graph-service.auth.jwt.issuer=myissuer"
    )
    
    content = output_file.read_text()
    assert "aerospike.graph-service.auth.enabled=true" in content


def test_generate_properties_auth_not_enabled_without_secret_and_issuer(tmp_path):
    """Test that auth.enabled is not set when both are None"""
    output_file = tmp_path / "test.properties"
    
    properties = [
        "aerospike.namespace=test",
    ]
    
    generate_properties(
        properties=properties,
        output_properties_file=str(output_file),
        auth_jwt_secret=None,
        auth_jwt_issuer=None
    )
    
    content = output_file.read_text()
    assert "aerospike.graph-service.auth.enabled=true" not in content


def test_generate_properties_empty_list(tmp_path):
    """Test with empty properties list"""
    output_file = tmp_path / "test.properties"
    
    generate_properties(
        properties=[],
        output_properties_file=str(output_file),
        auth_jwt_secret=None,
        auth_jwt_issuer=None
    )
    
    assert output_file.exists()
    content = output_file.read_text()
    # Should still have gremlin.graph
    assert "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph" in content
    assert "aerospike.graph-service.auth.enabled" not in content