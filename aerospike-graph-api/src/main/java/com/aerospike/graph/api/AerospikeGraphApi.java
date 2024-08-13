package com.aerospike.graph.api;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph.Features;

public interface AerospikeGraphApi extends AutoCloseable {
    static AerospikeGraphApiBuilder builder() {
        return new AerospikeGraphApiBuilder();
    }

    GraphTraversalSource traversal();

    Configuration configuration();

    Features features();
}
