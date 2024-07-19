package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.Iterators;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyMergeEdgeStepTest {
    static FireflyGraph SETUP_GRAPH;

    private FireflyVertex v1;
    private FireflyVertex v2;
    private FireflyEdge e1;
    private FireflyEdge e2;
    private FireflyEdge e3;
    private FireflyEdge e4;

    @BeforeClass
    static public void beforeClass() {
        SETUP_GRAPH = getGraphWithCacheSize(10000);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
    }

    @AfterClass
    static public void afterClass() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Test
    public void testNoSupernodes() {
        final FireflyGraph graph = getGraphWithCacheSize(1000);
        try {
            setupGraph(graph);
            assertLabel(graph);
            assertFilterable(graph);
            assertNotFilterable(graph);
            assertCompound(graph);
        } finally {
            graph.getBaseGraph().dropDatabase(graph, false);
            graph.close();
        }
    }

    @Test
    public void testOutSupernode() {
        final FireflyGraph graph = getGraphWithCacheSize(5);
        try {
            setupGraph(graph);
            assertLabel(graph);
            assertFilterable(graph);
            assertNotFilterable(graph);
            assertCompound(graph);
        } finally {
            graph.getBaseGraph().dropDatabase(graph, false);
            graph.close();
        }
    }

    @Test
    public void testBothSupernode() {
        final FireflyGraph graph = getGraphWithCacheSize(0);
        try {
            setupGraph(graph);
            assertLabel(graph);
            assertFilterable(graph);
            assertNotFilterable(graph);
            assertCompound(graph);
        } finally {
            graph.getBaseGraph().dropDatabase(graph, false);
            graph.close();
        }
    }

    @Test
    public void testConcurrentWriting() throws InterruptedException {
        final FireflyGraph graph = getGraphWithCacheSize(100);
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v1 = g.addV("v1").next();
            final Vertex v2 = g.addV("v2").next();
            final Vertex v3 = g.addV("v3").next();
            for (int i = 0; i < 50; i++) {
                g.addE("test").property("foo", i).from(v1).to(v3).iterate();
            }
            final List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                final Thread task = new Thread(() -> {
                    final GraphTraversalSource threadG = graph.traversal();
                    for (int j = 0; j < 100; j++) {
                        final Map<Object, Object> mergeMap = new HashMap<>();
                        mergeMap.put(Direction.OUT, new ReferenceVertex(v1.id()));
                        mergeMap.put(Direction.IN, new ReferenceVertex(v2.id()));
                        mergeMap.put(T.label, "test");
                        mergeMap.put("foo", j);
                        threadG.mergeE(mergeMap).iterate();
                    }
                });
                threads.add(task);
            }
            for (final Thread thread : threads) {
                thread.start();
            }
            for (final Thread thread : threads) {
                thread.join();
            }
            Assert.assertEquals(150, (long) g.V().hasLabel("v1").outE().count().next());
            Assert.assertEquals(100, (long) g.V().hasLabel("v2").inE().count().next());
            g.V().drop().iterate();
            threads.clear();

            final Vertex v11 = g.addV("v11").next();
            final Vertex v22 = g.addV("v22").next();
            final Vertex v33 = g.addV("v33").next();
            for (int i = 0; i < 50; i++) {
                g.addE("test").property("foo", i).from(v33).to(v22).iterate();
            }
            for (int i = 0; i < 3; i++) {
                final Thread task = new Thread(() -> {
                    final GraphTraversalSource threadG = graph.traversal();
                    for (int j = 0; j < 100; j++) {
                        final Map<Object, Object> mergeMap = new HashMap<>();
                        mergeMap.put(Direction.OUT, new ReferenceVertex(v11.id()));
                        mergeMap.put(Direction.IN, new ReferenceVertex(v22.id()));
                        mergeMap.put(T.label, "test");
                        mergeMap.put("foo", j);
                        threadG.mergeE(mergeMap).iterate();
                    }
                });
                threads.add(task);
            }
            for (final Thread thread : threads) {
                thread.start();
            }
            for (final Thread thread : threads) {
                thread.join();
            }
            Assert.assertEquals(100, (long) g.V().hasLabel("v11").outE().count().next());
            Assert.assertEquals(150, (long) g.V().hasLabel("v22").inE().count().next());
        } finally {
            graph.getBaseGraph().dropDatabase(graph, false);
            graph.close();
        }
    }

    private void setupGraph(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        v1 = (FireflyVertex) g.addV("v1").next();
        v2 = (FireflyVertex) g.addV("v2").next();
        final Vertex v3 = g.addV("v3").next();

        g.addE("label1").from(v1).to(v3).iterate();
        e1 = (FireflyEdge) g.addE("label1").property("filterable", 1).property("notfilterable", true).from(v1).to(v2).next();
        g.addE("label1").from(v1).to(v3).iterate();
        e2 = (FireflyEdge) g.addE("label1").property("filterable", 2).property("notfilterable", false).from(v1).to(v2).next();
        g.addE("label1").from(v1).to(v3).iterate();
        e3 = (FireflyEdge) g.addE("label2").property("filterable", 1).property("notfilterable", true).from(v1).to(v2).next();
        g.addE("label1").from(v1).to(v3).iterate();
        e4 = (FireflyEdge) g.addE("label2").property("filterable", 2).property("notfilterable", false).from(v1).to(v2).next();
        g.addE("label1").from(v1).to(v3).iterate();
    }

    private void assertLabel(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        Assert.assertEquals(9, Iterators.size(g.E()));
        final Map<Object, Object> mergeMap = new HashMap<>();
        mergeMap.put(Direction.OUT, new ReferenceVertex(v1.id()));
        mergeMap.put(Direction.IN, new ReferenceVertex(v2.id()));
        mergeMap.put(T.label, "label1");
        var traversal = g.mergeE(mergeMap);
        int mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertTrue(edge.id().equals(e1.id()) || edge.id().equals(e2.id()));
            mergeCount++;
        }
        Assert.assertEquals(2, mergeCount);
        Assert.assertEquals(9, Iterators.size(g.E()));

        mergeMap.put(T.label, "new");
        traversal = g.mergeE(mergeMap);
        mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertEquals("new", edge.label());
            mergeCount++;
        }
        Assert.assertEquals(1, mergeCount);
        Assert.assertEquals(10, Iterators.size(g.E()));

        g.E().hasLabel("new").drop().iterate();
    }

    private void assertFilterable(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        Assert.assertEquals(9, Iterators.size(g.E()));
        final Map<Object, Object> mergeMap = new HashMap<>();
        mergeMap.put(Direction.OUT, new ReferenceVertex(v1.id()));
        mergeMap.put(Direction.IN, new ReferenceVertex(v2.id()));
        mergeMap.put("filterable", 1);
        var traversal = g.mergeE(mergeMap);
        int mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertTrue(edge.id().equals(e1.id()) || edge.id().equals(e3.id()));
            mergeCount++;
        }
        Assert.assertEquals(2, mergeCount);
        Assert.assertEquals(9, Iterators.size(g.E()));

        mergeMap.put("filterable", 3);
        traversal = g.mergeE(mergeMap);
        mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertEquals(3, edge.property("filterable").value());
            mergeCount++;
        }
        Assert.assertEquals(1, mergeCount);
        Assert.assertEquals(10, Iterators.size(g.E()));

        g.E().has("filterable", 3).drop().iterate();
    }

    private void assertNotFilterable(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        Assert.assertEquals(9, Iterators.size(g.E()));
        final Map<Object, Object> mergeMap = new HashMap<>();
        mergeMap.put(Direction.OUT, new ReferenceVertex(v1.id()));
        mergeMap.put(Direction.IN, new ReferenceVertex(v2.id()));
        mergeMap.put("notfilterable", false);
        var traversal = g.mergeE(mergeMap);
        int mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertTrue(edge.id().equals(e2.id()) || edge.id().equals(e4.id()));
            mergeCount++;
        }
        Assert.assertEquals(2, mergeCount);
        Assert.assertEquals(9, Iterators.size(g.E()));

        mergeMap.put("notfilterablenew", false);
        traversal = g.mergeE(mergeMap);
        mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertFalse((Boolean) edge.property("notfilterablenew").value());
            mergeCount++;
        }
        Assert.assertEquals(1, mergeCount);
        Assert.assertEquals(10, Iterators.size(g.E()));

        g.E().has("notfilterablenew", false).drop().iterate();
    }

    private void assertCompound(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        Assert.assertEquals(9, Iterators.size(g.E()));
        final Map<Object, Object> mergeMap = new HashMap<>();
        mergeMap.put(Direction.OUT, new ReferenceVertex(v1.id()));
        mergeMap.put(Direction.IN, new ReferenceVertex(v2.id()));
        mergeMap.put(T.label, "label2");
        mergeMap.put("filterable", 1);
        mergeMap.put("notfilterable", true);
        var traversal = g.mergeE(mergeMap);
        int mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertEquals(e3.id(), edge.id());
            mergeCount++;
        }
        Assert.assertEquals(1, mergeCount);
        Assert.assertEquals(9, Iterators.size(g.E()));

        mergeMap.put(T.label, "label2");
        mergeMap.put("filterable", 1);
        mergeMap.put("notfilterable", false);
        traversal = g.mergeE(mergeMap);
        mergeCount = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertEquals("label2", edge.label());
            Assert.assertEquals(1, edge.property("filterable").value());
            Assert.assertFalse((Boolean) edge.property("notfilterable").value());
            mergeCount++;
        }
        Assert.assertEquals(1, mergeCount);
        Assert.assertEquals(10, Iterators.size(g.E()));
        Assert.assertEquals(3, Iterators.size(g.E().hasLabel("label2")));
        Assert.assertEquals(3, Iterators.size(g.E().has("filterable", 1)));
        Assert.assertEquals(3, Iterators.size(g.E().has("notfilterable", false)));
    }

    static private FireflyGraph getGraphWithCacheSize(final int cacheSize) {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, String.valueOf(cacheSize));
        return FireflyGraph.open(config);
    }
}
