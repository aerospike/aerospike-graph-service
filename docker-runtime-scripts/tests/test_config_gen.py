import pytest
import textwrap
from graph_config.configure_aerospike_graph import main


def test_main_generates_yaml_and_properties(tmp_path, monkeypatch):
    input_properties = tmp_path / "input.properties"
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "out.yaml"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    java_opts_file = tmp_path / "java_options"

    # Minimal config files for testing
    default_yaml.write_text(textwrap.dedent("""\
        host: localhost
        port: 8182
    """))

    input_properties.write_text(textwrap.dedent("""\
        aerospike.graph-service.heap.max=512m
        aerospike.graph-service.heap.min=256m
        aerospike.namespace=test
    """))

    monkeypatch.setenv("JAVA_OPTIONS", "")

    main(
        str(input_properties),
        str(default_yaml),
        str(output_yaml),
        str(conf_dir),
        str(java_opts_file),
    )

    assert output_yaml.exists()
    graph_props = (conf_dir / "aerospike-graph-graph.properties")
    assert graph_props.exists()
    assert java_opts_file.exists()

    # Check properties content
    contents = graph_props.read_text()
    assert "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph" in contents
    assert "aerospike.namespace=test" in contents

    # Check YAML contains graphs block
    yaml_text = output_yaml.read_text()
    assert "graphs:" in yaml_text
    assert "graph:" in yaml_text


def test_main_invalid_properties_format(tmp_path, monkeypatch):
    """Test that invalid property format raises an error"""
    input_properties = tmp_path / "input.properties"
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "out.yaml"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    java_opts_file = tmp_path / "java_options"

    default_yaml.write_text("host: localhost\n")
    
    # Properties without =
    input_properties.write_text(textwrap.dedent("""\
        aerospike.namespace=test
        invalid_property_without_equals
        aerospike.hosts=localhost:3000
    """))

    monkeypatch.setenv("JAVA_OPTIONS", "")

    with pytest.raises(Exception) as exc:
        main(
            str(input_properties),
            str(default_yaml),
            str(output_yaml),
            str(conf_dir),
            str(java_opts_file),
        )
    
    assert "Invalid properties found" in str(exc.value)
    assert "invalid_property_without_equals" in str(exc.value)


def test_main_invalid_property_not_starting_with_aerospike(tmp_path, monkeypatch):
    """Test that properties not starting with 'aerospike' are invalid"""
    input_properties = tmp_path / "input.properties"
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "out.yaml"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    java_opts_file = tmp_path / "java_options"

    default_yaml.write_text("host: localhost\n")
    
    # Property not starting with aerospike or gremlin.graph
    input_properties.write_text(textwrap.dedent("""\
        aerospike.namespace=test
        invalid.property=value
    """))

    monkeypatch.setenv("JAVA_OPTIONS", "")

    with pytest.raises(Exception) as exc:
        main(
            str(input_properties),
            str(default_yaml),
            str(output_yaml),
            str(conf_dir),
            str(java_opts_file),
        )
    
    assert "Invalid properties found" in str(exc.value)
    assert "invalid.property=value" in str(exc.value)


def test_main_multiple_graphs(tmp_path, monkeypatch):
    """Test main() with multiple named graphs"""
    input_properties = tmp_path / "input.properties"
    default_yaml = tmp_path / "default.yaml"
    output_yaml = tmp_path / "out.yaml"
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    java_opts_file = tmp_path / "java_options"

    default_yaml.write_text("host: localhost\n")
    
    # Properties file with multiple graphs
    input_properties.write_text(textwrap.dedent("""\
        aerospike.graph-service.graphs=graph1,graph2
        aerospike.namespace=common
        graph1.namespace=graph1_namespace
        graph1.hosts=localhost:3000
        graph2.namespace=graph2_namespace
        graph2.hosts=localhost:3001
    """))

    monkeypatch.setenv("JAVA_OPTIONS", "")

    main(
        str(input_properties),
        str(default_yaml),
        str(output_yaml),
        str(conf_dir),
        str(java_opts_file),
    )

    # Check that properties files were created for each graph
    graph1_props = conf_dir / "aerospike-graph-graph1.properties"
    graph2_props = conf_dir / "aerospike-graph-graph2.properties"
    
    assert graph1_props.exists()
    assert graph2_props.exists()

    # Check graph1 properties
    graph1_content = graph1_props.read_text()
    assert "aerospike.namespace=common" in graph1_content

    assert "namespace=graph1_namespace" in graph1_content
    assert "hosts=localhost:3000" in graph1_content

    assert "aerospike.graph.id=graph1" in graph1_content

    # Check graph2 properties
    graph2_content = graph2_props.read_text()
    assert "aerospike.namespace=common" in graph2_content

    assert "namespace=graph2_namespace" in graph2_content

    assert "aerospike.graph.id=graph2" in graph2_content

    # Check YAML contains both graphs
    yaml_text = output_yaml.read_text()
    assert "graph1:" in yaml_text
    assert "graph2:" in yaml_text
    assert "aerospike-graph-graph1.properties" in yaml_text
    assert "aerospike-graph-graph2.properties" in yaml_text
    