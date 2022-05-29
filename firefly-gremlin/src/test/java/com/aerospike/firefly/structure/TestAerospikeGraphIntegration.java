package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkResults;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.identity;
import static org.apache.tinkerpop.gremlin.structure.T.key;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeGraphIntegration {

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    private AerospikeConnection db;
    private FireflyGraph graph;


    @BeforeEach
    void openGraph() {
        this.db = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(config),
                ConfigurationHelper.aerospikePort(config),
                ConfigurationHelper.aerospikeNamespace(config));
        graph = new FireflyGraph(config);
        db.dropDatabase();
    }

    @AfterEach
    void closeGraphClearData() throws Exception {
        db.dropDatabase();
        graph.close();
    }

    @Test
    void testReadWriteRemovePropertyFromVertex() {
        FireflyVertex vertex = new FireflyVertex(null, 1l, "label", graph);
        String value = "b";
        String key = "bKey";
        FireflyProperty<String> p = new FireflyProperty<>(vertex, key, value);
        db.writeProperty(vertex, key, value);

        Property<String> readback = db.readProperty(vertex, key);
        assertEquals(p.key(), readback.key());
        assertEquals(p.value(), readback.value());

        db.removeProperty(vertex, key);
        boolean success = false;
        try {
            Property<String> gone = db.readProperty(vertex, key);
        } catch (NoSuchElementException nse) {
            success = true;
        }
        assertTrue(success);
    }

    @Test
    void testReadWriteRemoveGraphVariables() throws InterruptedException {
        graph.variables().set("this", "that");
        assertEquals("that", graph.variables().get("this").get().toString());
        assertEquals("this", graph.variables().keys().iterator().next());
        graph.variables().remove("this");
        assertFalse(graph.variables().keys().iterator().hasNext());
    }

    @Test
    void testReadWriteVertexProperty() {
        db.writeVertex(graph, 2l, "aVertexLabel");
        FireflyVertex vertex = db.readVertex(graph, 2l);
        VertexProperty<String> p = new FireflyVertexProperty<>(graph.vertexPropertyIdManager.getNextId(graph), vertex, "a", "b");
        db.writeVertexPropertyList(vertex, "aKey", List.of(p));
        Map<String, List<VertexProperty>> readBack = db.readVertexProperties(vertex);
        List<VertexProperty> aValue = readBack.get("aKey");
        assertNotEquals(aValue, null);
        assertEquals(aValue.get(0), p);
    }

    @Test
    void testReadWriteRemoveVertexPropertyTraversal() {
        GraphTraversalSource g = graph.traversal();
        Vertex v = g.addV().property("a", "b").next();
        assertEquals("b", g.V(v.id()).properties("a").value().next());
        g.V(v.id()).properties("a").next().remove();
        boolean success = false;
        try {
            g.V(v.id()).properties("a").value().next();
        } catch (NoSuchElementException nse) {
            success = true;
        }
        assertTrue(success);
    }

    @Test
    void testReadWriteVertex() {
        db.writeVertex(graph, 2l, "aVertexLabel");
        FireflyVertex v = db.readVertex(graph, 2l);
        assertEquals(v.label(), "aVertexLabel");
    }

    @Test
    void testVertexIterator() {
        List<Long> usedIds = new ArrayList<>();
        LongStream.range(0, 10).forEach(l -> {
            Long next = (Long) graph.vertexIdManager.getNextId(graph);
            usedIds.add(next);
            db.writeVertex(graph, next, "aVertexLabel");
        });
        final AtomicLong ctr = new AtomicLong(0);
        new FireflyVertexIterator<Long>(graph, usedIds.iterator()).forEachRemaining(v -> {
            ctr.addAndGet(1);
            assertEquals("aVertexLabel", v.label());
        });
        assertEquals(10, ctr.get());
    }

    @Test
    void testGraph() {
        Vertex v = graph.addVertex();
        v.property("this", "that");
        Vertex thing = graph.vertices(v.id()).next();
        assertEquals("that", thing.property("this").value().toString());
    }

    @Test
    void testGraphTraversal() {
        GraphTraversalSource g = graph.traversal();
        g.addV("herring").property("color", "white").next();
        Vertex thing = g.V().next();
        assertEquals("white", g.V().hasLabel("herring").values("color").next());
        GraphTraversal<Vertex, Vertex> i = g.V();
        while (i.hasNext()) {
            assertNotEquals(i.next(), null);
        }
    }

    @Test
    void testTraversalIterator() {
        GraphTraversalSource g = graph.traversal();
        g.addV("puppy").property("color", "red").next();
        Vertex thing = g.V().next();
        GraphTraversal<Vertex, Vertex> i = g.V();
        while (i.hasNext()) {
            assertNotEquals(i.next(), null);
        }
    }

    @Test
    void testRemoveVertexTraversal() {
        GraphTraversalSource g = graph.traversal();
        g.addV("puppy").property("color", "brown").next();
        assertTrue(g.V().hasNext());
        g.V().drop().iterate();
        if (g.V().hasNext()) {
            Vertex it = g.V().next();
            fail();
        }
    }

    @Test
    void testWriteMultipleThenIterate() {
        GraphTraversalSource g = graph.traversal();
        g.addV("penguin").property("color", "red").next();
        assertEquals("red", g.V().hasLabel("penguin").next().values("color").next());
    }

    @Test
    void testWriteThenDrop() {
        GraphTraversalSource g = graph.traversal();
        IntStream.range(0, 10).forEach(i -> {
            g.addV().next();
        });
        assertTrue(g.V().count().next() > 0);
        db.dropDatabase();
        assertEquals(0, (long) g.V().count().next());
    }


    @Test
    void testWrite2VertexWithEdge() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        assertEquals(2, g.V(fruit.id()).inE().count().next());
    }

    @Test
    void testWrite2VertexWithEdgeThenRemove() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        assertEquals(2, g.V(fruit.id()).inE().count().next());
        g.V(s1.id()).outE().drop().iterate();
        if (g.V(fruit.id()).outE().count().next() > 0) {
            Edge a = g.V(fruit.id()).outE().next();
            fail();
        }
    }

    @Test
    void testReadWriteRemoveEdgeProperty() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        assertEquals(2, g.V(fruit.id()).inE().count().next());
        Property<Object> prop = g.V(fruit.id()).inE().next().properties("this").next();
        assertEquals("that", prop.value());
    }

    @Test
    void testEdgeLabel() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        assertEquals(2, g.E().hasLabel("IsA").count().next());
        assertEquals(2, g.V().outE().hasLabel("IsA").count().next());
    }

    @Test
    void testEdgeLabel2() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        assertEquals(1, g.V().has("color", "yellow").outE().count().next());
    }
    @Test
    void g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name(){
        GraphTraversalSource g = graph.traversal();
        loadKryoData(g,"tinkerpop-modern.kryo");
        GraphTraversal<Vertex, Object> traversal = g.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).values("name");
        checkResults(Arrays.asList("lop", "ripple", "josh", "vadas", "vadas"), traversal);
    }

    private void loadKryoData(GraphTraversalSource g, String resourceName){
        final Path tempPath;
        try {
            tempPath = Files.createTempDirectory("firefly-test").toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempPath.toFile().deleteOnExit();
        Util.copyResourceToDirectory(resourceName, tempPath);
        String resourcePath = tempPath.resolve(resourceName).toAbsolutePath().toString();
        g.io(resourcePath).read().iterate();
    }
    @Test
    void testGrateful() throws IOException {
        GraphTraversalSource g = graph.traversal();
        GraphTraversalSource g2 = TinkerFactory.createGratefulDead().traversal();
        loadKryoData(g,"grateful-dead.kryo");
        Long x1 = g.V().count().next();
        Long x2 = g2.V().count().next();
        assertEquals(x2, x1);
        Long x = g.V().has("name", "CANT COME DOWN").outE().count().next();
        Long y = g2.V().has("name", "CANT COME DOWN").outE().count().next();
        assertEquals(x, y);
        assertEquals(
                g2.V().has("name", "CANT COME DOWN").outE().inV().count().next(),
                g.V().has("name", "CANT COME DOWN").outE().inV().count().next());
    }

    @Test
    void noNext() {
        try {
            graph.edges(10000l).next();
            fail("Call to g.edges(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, IsInstanceOf.instanceOf(NoSuchElementException.class));
        }
    }

    @Test
    void testTree() {
        int branchSize = 11;
        final Vertex start = graph.addVertex();
        for (int i = 0; i < branchSize; i++) {
            final Vertex a = graph.addVertex();
            start.addEdge("test1", a);
            for (int j = 0; j < branchSize; j++) {
                final Vertex b = graph.addVertex();
                a.addEdge("test2", b);
                for (int k = 0; k < branchSize; k++) {
                    final Vertex c = graph.addVertex();
                    b.addEdge("test3", c);
                }
            }
        }
        assertEquals(0L, IteratorUtils.count(start.edges(Direction.IN, new String[0])));
        assertEquals((long) branchSize, IteratorUtils.count(start.edges(Direction.OUT, new String[0])));
        Iterator var9 = IteratorUtils.list(start.edges(Direction.OUT, new String[0])).iterator();

        while (var9.hasNext()) {
            Edge a = (Edge) var9.next();
            Assert.assertEquals("test1", a.label());

            Assert.assertEquals((long) branchSize, IteratorUtils.count(a.inVertex().vertices(Direction.OUT, new String[0])));
            Assert.assertEquals(1L, IteratorUtils.count(a.inVertex().vertices(Direction.IN, new String[0])));
            Iterator var12 = IteratorUtils.list(a.inVertex().edges(Direction.OUT, new String[0])).iterator();

            while (var12.hasNext()) {
                Edge b = (Edge) var12.next();
                Assert.assertEquals("test2", b.label());
                Assert.assertEquals((long) branchSize, IteratorUtils.count(b.inVertex().vertices(Direction.OUT, new String[0])));
                Assert.assertEquals(1L, IteratorUtils.count(b.inVertex().vertices(Direction.IN, new String[0])));
                Iterator var14 = IteratorUtils.list(b.inVertex().edges(Direction.OUT, new String[0])).iterator();

                while (var14.hasNext()) {
                    Edge c = (Edge) var14.next();
                    Assert.assertEquals("test3", c.label());
                    Assert.assertEquals(0L, IteratorUtils.count(c.inVertex().vertices(Direction.OUT, new String[0])));
                    Assert.assertEquals(1L, IteratorUtils.count(c.inVertex().vertices(Direction.IN, new String[0])));
                }
            }
        }
    }

    @Test
    void testEdgeIdScan() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        Iterator<Object> i = db.getOutEdgeIdsFromVertexByScan((FireflyVertex) fruit);
        assertTrue(i.hasNext());
        List<Object> x = List.of(lemon.edges(Direction.OUT).next().id(), lime.edges(Direction.OUT).next().id());
        Object next = i.next();
        assertTrue(x.contains(next));
        assertTrue(x.contains(next));
    }

    @Test
    @Disabled
    void testDetached() {

//        TinkerGraph graph2 = TinkerFactory.createModern();
//        GraphTraversalSource g2 = graph2.traversal();
//        AtomicLong ctr2 = new AtomicLong(0);
//        Vertex starVertex2 = graph2.addVertex(T.label, "person", "name", "stephen", "name", "spmallete");
//        starVertex2.property("acl", true, "timestamp", ctr2.addAndGet(1), "creator", "marko");
////        for (int i = 0; i < 100; i++) {
////            starVertex.addEdge("knows", graph.addVertex(T.label,"person", "name", new UUID(ctr.addAndGet(1), ctr.addAndGet(1)).toString(), "since", ctr.addAndGet(1)));
////            graph.addVertex(T.label, "project").addEdge("developedBy", starVertex, "public", false);
////        }
//        final DetachedVertex detachedVertex2 = DetachedFactory.detach(g2.V(starVertex2.id()).next(), true);
//        g2.V(starVertex2.id()).drop();
//        final Vertex createdVertex2 = detachedVertex2.attach(Attachable.Method.create(graph2));
//        List<VertexProperty<Object>> starVertexProperties2 = IteratorUtils.list(starVertex2.properties());
//        List<VertexProperty<Object>> detachedVertexProperties2 = IteratorUtils.list(detachedVertex2.properties());
//        List<VertexProperty<Object>> createdVertexProperties2 = IteratorUtils.list(createdVertex2.properties());
//        assertEquals(starVertexProperties2.size(),detachedVertexProperties2.size());
//        assertEquals(detachedVertexProperties2.size(),createdVertexProperties2.size());
//


        GraphTraversalSource g = graph.traversal();
        AtomicLong ctr = new AtomicLong(0);
        Vertex starVertex = graph.addVertex(T.label, "person", "name", "stephen", "name", "spmallete");
        starVertex.property("acl", true, "timestamp", ctr.addAndGet(1), "creator", "marko");
//        for (int i = 0; i < 100; i++) {
//            starVertex.addEdge("knows", graph.addVertex(T.label,"person", "name", new UUID(ctr.addAndGet(1), ctr.addAndGet(1)).toString(), "since", ctr.addAndGet(1)));
//            graph.addVertex(T.label, "project").addEdge("developedBy", starVertex, "public", false);
//        }
        final DetachedVertex detachedVertex = DetachedFactory.detach(g.V(starVertex.id()).next(), true);
        final Vertex createdVertex = detachedVertex.attach(Attachable.Method.create(graph));
        List<VertexProperty<Object>> starVertexProperties = IteratorUtils.list(starVertex.properties());
        List<VertexProperty<Object>> detachedVertexProperties = IteratorUtils.list(detachedVertex.properties());
        List<VertexProperty<Object>> createdVertexProperties = IteratorUtils.list(createdVertex.properties());
        assertEquals(starVertexProperties.size(), detachedVertexProperties.size());
        assertEquals(detachedVertexProperties.size(), createdVertexProperties.size());

        TestHelper.validateVertexEquality(detachedVertex, createdVertex, false);
        TestHelper.validateVertexEquality(detachedVertex, starVertex, false);

    }

    @Test
    @Disabled
    void testVertexPropertyComplex() {
        GraphTraversalSource g = graph.traversal();
        g.addV("human")
                .property("name", "grant")
                .property("age", 34)
                .property("location", "ABQ").next();
        List<VertexProperty<String>> list = IteratorUtils.list(g.V().has("name", "grant").next().properties());
        assertEquals(3, list.size());
        g.V().has("name", "grant").properties("age").drop();
        List<VertexProperty<String>> list2 = IteratorUtils.list(g.V().has("name", "grant").next().properties());

        assertEquals(2, list2.size());
    }

    private void loadGraphmlFromResources(Graph graph, String resourceName) throws IOException {
        final Path tempPath;
        try {
            tempPath = Files.createTempDirectory("firefly-test").toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempPath.toFile().deleteOnExit();
        Util.copyResourceToDirectory(resourceName, tempPath);
        String resourcePath = tempPath.resolve(resourceName).toAbsolutePath().toString();
        graph.io(graphml()).readGraph(resourcePath);
    }
    @Test
    void airRoutesTest() throws IOException {
        loadGraphmlFromResources(graph,"air-routes-small.graphml");
        GraphTraversalSource g = graph.traversal();
        Map<String, Object> res = g.V().has("airport", "code", "DFW").propertyMap().next();
        Map<Object, Object> stuff = g.V().hasLabel("airport").
                properties("runways", "longest").
                group().by(key).by(value().sum()).next();
        System.out.println(res);
        System.out.println(stuff);

    }



}
