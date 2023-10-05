package com.aerospike.firefly.io.utils;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PropertyInsertionBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(PropertyInsertionBenchmark.class);
    private final GraphTraversalSource g;
    private final int propertyCount;
    private final int edgePackSize;
    private final int reportingGroupSize;
    private final List<Edge> edges = new ArrayList<>();
    private final List<Metric> vertexMetrics = new ArrayList<>();
    private final List<Metric> edgeMetrics = new ArrayList<>();
    private Vertex from;

    public PropertyInsertionBenchmark(final GraphTraversalSource g, final int propertyCount, final int edgePackSize,
                                      final int reportingGroupSize) {
        this.g = g;
        this.propertyCount = propertyCount;
        this.edgePackSize = edgePackSize;
        this.reportingGroupSize = reportingGroupSize;

        final int phatEdgeSize = ((FireflyGraph) g.getGraph()).getBaseGraph().PHAT_EDGE_SIZE;
        if (this.edgePackSize > phatEdgeSize) {
            throw new IllegalArgumentException("Benchmark results are invalid if edgePackSize is greater than PHAT_EDGE_SIZE of graph: " + phatEdgeSize);
        }
        this.initialize();
    }

    private void initialize() {
        g.V().drop().iterate();
        this.from = g.addV("from").next();
        Vertex to = g.addV("to").next();

        for (int i = 0; i < edgePackSize; i++) {
            this.edges.add(this.from.addEdge("edge", to));
        }
    }

    public void benchmark() {
        addVertexProperties();
        addEdgeProperties();
        for (final Metric metric : this.vertexMetrics) {
            metric.printMetric();
        }
        for (final Metric metric : this.edgeMetrics) {
            metric.printMetric();
        }
    }

    private void addVertexProperties() {
        int setCount = 0;
        Metric currentMetric = new Metric(++setCount);
        int currentGroupCount = 0;
        for (int i = 0; i < propertyCount; i++) {
            final String propertyValue = String.valueOf(i);
            final String propertyKey = "property_" + propertyValue;
            final long start = System.nanoTime();
            this.from.property(propertyKey, propertyValue);
            currentMetric.addDatapoint(System.nanoTime() - start);
            if (++currentGroupCount >= reportingGroupSize) {
                this.vertexMetrics.add(currentMetric);
                currentMetric = new Metric(++setCount);
                currentGroupCount = 0;
            }
        }
        if (currentGroupCount != 0) {
            this.vertexMetrics.add(currentMetric);
        }
    }

    private void addEdgeProperties() {
        int edgeCount = 0;
        for (final Edge edge : this.edges) {
            int setCount = 0;
            Metric currentMetric = new Metric(++setCount, ++edgeCount);
            int currentGroupCount = 0;
            for (int i = 0; i < propertyCount; i++) {
                final String propertyValue = String.valueOf(i);
                final String propertyKey = "property_" + propertyValue;
                final long start = System.nanoTime();
                edge.property(propertyKey, propertyValue);
                currentMetric.addDatapoint(System.nanoTime() - start);
                if (++currentGroupCount >= reportingGroupSize) {
                    this.edgeMetrics.add(currentMetric);
                    currentMetric = new Metric(++setCount, edgeCount);
                    currentGroupCount = 0;
                }
            }
            if (currentGroupCount != 0) {
                this.edgeMetrics.add(currentMetric);
            }
        }
    }

    private static class Metric {
        private double min = Long.MAX_VALUE;
        private double max = Long.MIN_VALUE;
        private double sum = 0;
        private double datapoints = 0;
        private int setCount;
        private Integer edgeSetCount = null;

        public Metric(final int setCount) {
            this.setCount = setCount;
        }

        public Metric(final int setCount, final int edgeSetCount) {
            this.setCount = setCount;
            this.edgeSetCount = edgeSetCount;
        }

        public void addDatapoint(final double duration) {
            if (duration < min) {
                this.min = duration;
            }
            if (duration > max) {
                this.max = duration;
            }
            this.sum += duration;
            datapoints++;
        }

        public void printMetric() {
            if (this.edgeSetCount == null) {
                LOG.info("Vertex property insertion metrics of size: " + this.datapoints + "; set: " + this.setCount);
                LOG.info("Minimum ms: " + this.min / 1000000);
                LOG.info("Maximum ms: " + this.max / 1000000);
                LOG.info("Average ms: " + this.sum / this.datapoints / 1000000);
            } else {
                LOG.info("Edge " + edgeSetCount + " property insertion metrics of size: " + this.datapoints + "; set: " + this.setCount);
                LOG.info("Minimum ms: " + this.min / 1000000);
                LOG.info("Maximum ms: " + this.max / 1000000);
                LOG.info("Average ms: " + this.sum / this.datapoints / 1000000);
            }
        }
    }
}
