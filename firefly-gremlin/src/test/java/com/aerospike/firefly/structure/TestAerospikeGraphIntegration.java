package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.Util.loadKryoDataFromResources;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkResults;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.identity;
import static org.apache.tinkerpop.gremlin.structure.T.key;
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
        db.writeVertexPropertyList(vertex, "a", List.of(p));
        Map<String, List<VertexProperty>> readBack = db.readVertexProperties(vertex);
        List<VertexProperty> aValue = readBack.get("a");
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
    void g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name() {
        GraphTraversalSource g = graph.traversal();
        loadKryoDataFromResources(g, "tinkerpop-modern.kryo");

        TinkerGraph tg = TinkerFactory.createModern();
        GraphTraversalSource tgs = tg.traversal();
        List<Vertex> tgsimple = tgs.V().hasLabel("person").toList();
        List<Vertex> simple = g.V().hasLabel("person").toList();

        List<Vertex> tg2 = tgs.V().hasLabel("person").out("created").toList();
        List<Vertex> g2 = g.V().hasLabel("person").out("created").toList();

        List<Vertex> tgthing = tgs.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).toList();
        List<Vertex> thing = g.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).toList();
        GraphTraversal<Vertex, Object> tgtraversal = tgs.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).values("name");
        GraphTraversal<Vertex, Object> traversal = g.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).values("name");
        checkResults(Arrays.asList("lop", "ripple", "josh", "vadas", "vadas"), tgtraversal);
        checkResults(Arrays.asList("lop", "ripple", "josh", "vadas", "vadas"), traversal);
    }


    @Test
    public void g_V_both_properties_properties_dedup_count() {
        GraphTraversalSource g = graph.traversal();
        loadKryoDataFromResources(g, "tinkerpop-crew.kryo");

        TinkerGraph tg = TinkerFactory.createTheCrew();
        GraphTraversalSource tgs = tg.traversal();

        List<Vertex> tgeles = tgs.V().both().dedup().toList();
        List<? extends Property<Object>> tgelesProps = tgs.V().both().dedup().properties().toList();
        List<? extends Property<Object>> tgelesPropsProps = tgs.V().both().dedup().properties().properties().toList();

        List<? extends Property<Object>> list2 = tgs.V().both().properties().properties().dedup().toList();
        long nondedupCount2 = tgs.V().both().properties().properties().count().next();
        long count2 = tgs.V().both().properties().properties().dedup().count().next();

        List<Vertex> geles = g.V().both().dedup().toList();
        List<? extends Property<Object>> gelesProps = g.V().both().dedup().properties().toList();
        List<? extends Property<Object>> gelesPropsProps = g.V().both().dedup().properties().properties().toList();

        List<? extends Property<Object>> list = g.V().both().properties().properties().dedup().toList();
        long nondedupCount = g.V().both().properties().properties().count().next();
        long count = g.V().both().properties().properties().dedup().count().next();
        assertEquals(21L, count);
    }
    @Test
    public void g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
        GraphTraversalSource g = graph.traversal();
        loadKryoDataFromResources(g, "tinkerpop-crew.kryo");

        TinkerGraph tg = TinkerFactory.createTheCrew();
        GraphTraversalSource tgs = tg.traversal();

        long c1 = g.V().count().next();
        long c2 = tgs.V().count().next();
        assertEquals(c1, c2);
        Object mid = g.V().has("name", "marko").id().next();

        Map<String, Object> pm1 = g.V(mid).propertyMap().next();
        Map<String, Object> pm2 = tgs.V(mid).propertyMap().next();
        ArrayList<VertexProperty> pml1 = (ArrayList<VertexProperty>) pm1.get("location");
        ArrayList<VertexProperty> pml2 = (ArrayList<VertexProperty>) pm2.get("location");
        assertEquals(pml2.size(), pml1.size());
        List<? extends Property<Object>> gr1 = g.V().properties("location").toList();
        List<? extends Property<Object>> tgr1 = tgs.V().properties("location").toList();
        assertEquals(gr1.size(), tgr1.size());

        Traversal<Vertex, String> traversal = g.V().local(properties("location").order().by(T.value, Order.asc).range(0, 2)).value();
        Traversal<Vertex, String> tgtraversal = tgs.V().local(properties("location").order().by(T.value, Order.asc).range(0, 2)).value();
        checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), tgtraversal);
        checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), traversal);
    }




    @Test
    void testGrateful() throws IOException {
        GraphTraversalSource g = graph.traversal();
        GraphTraversalSource g2 = TinkerFactory.createGratefulDead().traversal();
        loadKryoDataFromResources(g, "grateful-dead.kryo");
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
        Iterator<Object> i = db.getInEdgeIdsFromVertexByScan((FireflyVertex) fruit);
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



    @Test
    void airRoutesTest() throws IOException {
        Util.loadGraphmlFromResources(graph, "air-routes-small.graphml");
        GraphTraversalSource g = graph.traversal();
        Map<String, Object> res = g.V().has("airport", "code", "DFW").propertyMap().next();
        Map<Object, Object> stuff = g.V().hasLabel("airport").
                properties("runways", "longest").
                group().by(key).by(value().sum()).next();
        System.out.println(res);
        System.out.println(stuff);
    }

}
