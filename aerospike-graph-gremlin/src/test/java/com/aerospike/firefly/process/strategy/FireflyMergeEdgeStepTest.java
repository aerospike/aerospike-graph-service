package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.Iterators;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

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
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, String.valueOf(100));
        final FireflyGraph graph = FireflyGraph.open(config);
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
            int v1OutECount = 0;
            GraphTraversal<?, Edge> t = g.V().hasLabel("v1").outE();
            while (t.hasNext()) {
                final Edge e = t.next();
                v1OutECount++;
            }
            Assert.assertEquals(150, v1OutECount);
            int v2InECount = 0;
            t = g.V().hasLabel("v2").inE();
            while (t.hasNext()) {
                final Edge e = t.next();
                v2InECount++;
            }
            Assert.assertEquals(100, v2InECount);
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
            int v11OutECount = 0;
            t = g.V().hasLabel("v11").outE();
            while (t.hasNext()) {
                final Edge e = t.next();
                v11OutECount++;
            }
            Assert.assertEquals(100, v11OutECount);
            int v22InECount = 0;
            t = g.V().hasLabel("v22").inE();
            while (t.hasNext()) {
                final Edge e = t.next();
                v22InECount++;
            }
            Assert.assertEquals(150, v22InECount);
        } finally {
            graph.getBaseGraph().dropDatabase(graph, false);
            graph.close();
        }
    }

    @Test
    public void testConcurrentWithOptions() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, String.valueOf(100));
        final FireflyGraph graph = FireflyGraph.open(config);
        try {
            final GraphTraversalSource g = graph.traversal();
            final ArrayList<Vertex> verticesA = new ArrayList<>();
            final ArrayList<Vertex> verticesB = new ArrayList<>();

            for (int i = 0; i < 1000; i++) {
                verticesA.add(g.addV("A").property(String.valueOf(i), i).next());
                verticesB.add(g.addV("B").property(String.valueOf(i), i).next());
            }

            Assert.assertEquals(2000, (long) g.V().count().next());
            Assert.assertEquals(0, (long) g.E().count().next());
            Assert.assertEquals(0, (long) g.V(verticesA.get(0)).outE().count().next());

            final Thread t0 = new Thread(new MergeERunnable(g, verticesA, verticesB));
            final Thread t1 = new Thread(new MergeERunnable(g, verticesA, verticesB));
            t0.start();
            t1.start();
            t0.join();
            t1.join();

            Assert.assertEquals(1000, (long) g.E().count().next());
            Assert.assertEquals(1000, (long) g.V(verticesA.get(0)).outE().count().next());
            var traversal = g.V(verticesA.get(0)).outE();
            while (traversal.hasNext()) {
                final Edge e = traversal.next();
                Assert.assertEquals(3, e.property("created").value());
            }
            int inECount = 0;
            for (final Vertex v : verticesB) {
                traversal = g.V(v).inE();
                Assert.assertTrue(traversal.hasNext());
                while (traversal.hasNext()) {
                    inECount++;
                    final Edge e = traversal.next();
                    Assert.assertEquals(3, e.property("created").value());
                }
            }
            Assert.assertEquals(1000, inECount);
        } finally {
            graph.getBaseGraph().dropDatabase(graph, false);
            graph.close();
        }
    }

    @Test
    public void testPropertiesUpdate() {
        final FireflyGraph graph = getGraphWithCacheSize(1000);
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV().next();
        final Vertex v2 = g.addV().next();

        final GraphTraversal<?, ?> onMatch = __.sideEffect(__.property("count", __.union(__.values("count"), __.constant(1)).sum()))
                .constant(Map.of());

        // should create edge and set Count == 1
        final Edge e1 = g.mergeE(asMap(Direction.OUT, v1.id(), Direction.IN, v2.id()))
                .option(Merge.onCreate, asMap("count", 1))
                .option(Merge.onMatch, onMatch).next();
        Assert.assertEquals(1, e1.property("count").value());
        Assert.assertEquals(v1.id(), e1.outVertex().id());
        Assert.assertEquals(v2.id(), e1.inVertex().id());

        // should update edge and set Count == 2
        final Edge e2 = g.mergeE(asMap(Direction.OUT, v1.id(), Direction.IN, v2.id()))
                .option(Merge.onCreate, asMap("count", 1))
                .option(Merge.onMatch, onMatch).next();
        Assert.assertEquals(2, e2.property("count").value());
        Assert.assertEquals(e1.id(), e2.id());
        Assert.assertEquals(v1.id(), e2.outVertex().id());
        Assert.assertEquals(v2.id(), e2.inVertex().id());

        graph.close();
    }

    private static class MergeERunnable implements Runnable {
        final private GraphTraversalSource g;
        final private ArrayList<Vertex> verticesA;
        final private ArrayList<Vertex> verticesB;

        private MergeERunnable(final GraphTraversalSource g, final ArrayList<Vertex> verticesA,
                               final ArrayList<Vertex> verticesB) {
            this.g = g;
            this.verticesA = verticesA;
            this.verticesB = verticesB;
        }

        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                final Map<Object, Object> mergeMap = new HashMap<>();
                final Map<Object, Object> matchMap = new HashMap<>();
                final Map<Object, Object> createMap = new HashMap<>();
                mergeMap.put(T.label, "connect");
                mergeMap.put(Direction.OUT, verticesA.get(0));
                mergeMap.put(Direction.IN, verticesB.get(i));
                matchMap.put("created", 3);
                createMap.put("created", 2);
                g.mergeE(mergeMap).option(Merge.onCreate, createMap).option(Merge.onMatch, matchMap).iterate();
            }
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
