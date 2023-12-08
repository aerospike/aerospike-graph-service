

def main():
    graph_class = "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph"
    with open("/opt/aerospike-graph/conf/aerospike-graph.properties", "r+") as graph_properties:
        if "gremlin.graph=" not in graph_properties.read().lower():
            graph_properties.write(f'\n{graph_class}')


if __name__ == "__main__":
    main()
