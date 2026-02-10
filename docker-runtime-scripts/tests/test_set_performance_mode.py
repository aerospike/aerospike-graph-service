from graph_config.configure_aerospike_graph import set_performance_mode

def test_set_performance_mode_defaults(monkeypatch):
    monkeypatch.setattr(
        "graph_config.configure_aerospike_graph.multiprocessing.cpu_count",
        lambda: 8,
    )

    props = []
    set_performance_mode(props)

    assert "aerospike.graph-service.threadPoolWorker=4" in props
    assert "aerospike.graph-service.gremlinPool=32" in props