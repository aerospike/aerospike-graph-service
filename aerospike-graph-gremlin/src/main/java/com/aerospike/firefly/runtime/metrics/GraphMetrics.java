package com.aerospike.firefly.runtime.metrics;

import com.aerospike.firefly.structure.FireflyGraph;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphMetrics {
    private static final Logger logger = LoggerFactory.getLogger(GraphMetrics.class);
    private final String supernodesTraversedMetric = MetricRegistry.name("supernode_traversed_count");
    private final FireflyGraph graph;

    public GraphMetrics(final FireflyGraph graph) {
        this.graph = graph;
    }

    public void start() {
        MetricManager.INSTANCE.getRegistry().register(supernodesTraversedMetric, (Gauge<Integer>) this::getSupernodesTraversed);
    }

    public void shutDown() {
        logger.info("Shutting down graph metrics.");
        MetricManager.INSTANCE.getRegistry().remove(supernodesTraversedMetric);
    }

    private int getSupernodesTraversed() {
        return graph.getSupernodesTraversed();
    }
}
