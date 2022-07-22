package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyVertexIterator;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkResults;
import static org.apache.tinkerpop.gremlin.process.traversal.IO.graphson;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeGraphIntegration {

    private static final Configuration config;
    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    private static AerospikeConnection db;
    private static FireflyGraph graph;


    @BeforeClass
    public static void openGraph() {
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
    }

    @Before
    public void clearGraph() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            if (graph.traversal().V().count().next() > 0 || graph.traversal().E().count().next() > 0)
                LoggerFactory.getLogger("clearGraph").warn("nonzero vertex or edge count at start of test");
            graph.traversal().V().drop().iterate();
        }
    }

    @AfterClass
    public static void closeGraphClearData() throws Exception {
        graph.traversal().V().drop().iterate();
        graph.close();
        db.close();
    }

    @Test
    public void testReadWriteRemovePropertyFromEdge() {
        FireflyVertex vertexA = (FireflyVertex) graph.addVertex("label");
        FireflyVertex vertexB = (FireflyVertex) graph.addVertex("label");
        String value = "b";
        String key = "bKey";
        FireflyEdge edge = (FireflyEdge) vertexA.addEdge("label", vertexB, key, value);
        String value2 = "c";
        String key2 = "cKey";
        FireflyProperty<String> p = new FireflyProperty<>(edge, key2, value2);
        db.elementBackend.writeProperty(edge.id, edge.getClass(), key2, value2);
        Property<String> readback = db.elementBackend.readProperty(edge, key2);
        assertEquals(p.key(), readback.key());
        assertEquals(p.value(), readback.value());
        db.elementBackend.removeProperty(edge, key);
        boolean success = false;
        try {
            Property<String> gone = db.elementBackend.readProperty(edge, key);
        } catch (NoSuchElementException nse) {
            success = true;
        }
        assertTrue(success);
    }

    @Test
    public void testReadWriteRemoveGraphVariables() throws InterruptedException {
        graph.variables().set("this", "that");
        assertEquals("that", graph.variables().get("this").get().toString());
        assertEquals("this", graph.variables().keys().iterator().next());
        graph.variables().remove("this");
        assertFalse(graph.variables().keys().iterator().hasNext());
    }

    @Test
    public void testReadWriteVertexProperty() {
        db.vertexBackend.writeVertex(graph, FireflyId.of(FireflyVertex.class, 2L), "aVertexLabel");
        FireflyVertex vertex = db.vertexBackend.readVertex(graph, FireflyId.of(FireflyVertex.class, 2L));
        FireflyId vpid = FireflyId.createFromManager(graph, FireflyVertexProperty.class);
        db.vpBackend.writeVertexProperty(vertex, vpid, "a", "a", "b");
        VertexProperty<Object> p = db.vpBackend.readVertexProperty(vertex, vpid);
        Map<String, List<VertexProperty>> readBack = db.vpBackend.readVertexProperties(vertex);
        List<VertexProperty> aValue = readBack.get("a");
        assertNotEquals(aValue, null);
        assertEquals(aValue.get(0), p);
    }

    @Test
    public void testReadWriteRemoveVertexPropertyTraversal() {
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
    public void testReadWriteVertex() {
        FireflyId id = FireflyId.createFromManager(graph, FireflyVertex.class);
        db.vertexBackend.writeVertex(graph, id, "aVertexLabel");
        FireflyVertex v = db.vertexBackend.readVertex(graph, id);
        assertEquals(v.label(), "aVertexLabel");
    }

    @Test
    public void testVertexIterator() {
        List<Long> usedIds = new ArrayList<>();
        LongStream.range(0, 10).forEach(l -> {
            FireflyId next = FireflyId.createFromManager(graph, FireflyVertex.class);
            usedIds.add((Long) next.value());
            db.vertexBackend.writeVertex(graph, next, "aVertexLabel");
        });
        final AtomicLong ctr = new AtomicLong(0);
        new FireflyVertexIterator<Long>(graph, usedIds.iterator()).forEachRemaining(v -> {
            ctr.addAndGet(1);
            assertEquals("aVertexLabel", v.label());
        });
        assertEquals(10, ctr.get());
    }

    @Test
    public void testGraph() {
        Vertex v = graph.addVertex();
        v.property("this", "that");
        Vertex thing = graph.vertices(v.id()).next();
        assertEquals("that", thing.property("this").value().toString());
    }

    @Test
    public void testGraphTraversal() {
        GraphTraversalSource g = graph.traversal();
        g.addV("herring").property("color", "white").next();
        assertNotNull(g.V().next());
        assertEquals("white", g.V().hasLabel("herring").values("color").next());
        GraphTraversal<Vertex, Vertex> i = g.V();
        while (i.hasNext()) {
            assertNotEquals(i.next(), null);
        }
    }

    @Test
    public void testTraversalIterator() {
        GraphTraversalSource g = graph.traversal();
        g.addV("puppy").property("color", "red").next();
        Vertex thing = g.V().next();
        GraphTraversal<Vertex, Vertex> i = g.V();
        while (i.hasNext()) {
            assertNotEquals(i.next(), null);
        }
    }

    @Test
    public void testRemoveVertexTraversal() {
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
    public void testWriteMultipleThenIterate() {
        GraphTraversalSource g = graph.traversal();
        g.addV("penguin").property("color", "red").next();
        assertEquals("red", g.V().hasLabel("penguin").next().values("color").next());
    }

    @Test
    public void testWriteThenDrop() {
        GraphTraversalSource g = graph.traversal();
        IntStream.range(0, 10).forEach(i -> g.addV().next());
        assertTrue(g.V().count().next() > 0);
        g.V().drop().iterate();
        assertEquals(0, (long) g.V().count().next());
    }


    @Test
    public void testWrite2VertexWithEdge() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("a","b").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        List<Edge> things = g.E().has("a", "b").toList();
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
    }

    @Test
    public void testWrite2VertexWithEdgeThenRemove() {
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
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        g.V(s1.id()).outE().drop().iterate();
        if (g.V(fruit.id()).outE().count().next() > 0) {
            Edge a = g.V(fruit.id()).outE().next();
            fail();
        }
    }


    @Test
    public void testEdgeNumericIndexLong() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon")
                .property("color", "yellow")
                .property("type", "plant")
                .next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n",3L).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Edge> nEdge = g.E().has("n", 3L).toList();
        assertEquals(2,nEdge.size());
        List<Edge> ltnEdge = g.E().has("n",  P.lt(4L)).toList();
        assertEquals(2,ltnEdge.size());
        List<Edge> gtnEdge = g.E().has("n",  P.gt(1L)).toList();
        assertEquals(2,gtnEdge.size());
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        g.V(s1.id()).outE().drop().iterate();
        if (g.V(fruit.id()).outE().count().next() > 0) {
            Edge a = g.V(fruit.id()).outE().next();
            fail();
        }
    }


    @Test
    public void testVertexNumericIndexLong() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon")
                .property("color", "yellow")
                .property("type", "plant")
                .property("spots",3L).next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .property("spots",2L).next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n",3).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> twoSpots = g.V().has("spots", 2L).toList();
        List<Vertex> slt = g.V().has("spots", P.lt(4L)).toList();
        List<Vertex> sgt = g.V().has("spots", P.gt(1L)).toList();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        g.V(s1.id()).outE().drop().iterate();
        if (g.V(fruit.id()).outE().count().next() > 0) {
            Edge a = g.V(fruit.id()).outE().next();
            fail();
        }
    }
    @Test
    public void testVertexNumericIndexInteger() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon")
                .property("color", "yellow")
                .property("type", "plant")
                .property("spots",3).next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .property("spots",2).next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n",3).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> twoSpots = g.V().has("spots", 2).toList();
        assertEquals(1,twoSpots.size());
        List<Vertex> slt = g.V().has("spots", P.lt(4)).toList();
        assertEquals(2,slt.size());
        List<Vertex> sgt = g.V().has("spots", P.gt(1)).toList();
        assertEquals(2,sgt.size());
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        g.V(s1.id()).outE().drop().iterate();
        if (g.V(fruit.id()).outE().count().next() > 0) {
            Edge a = g.V(fruit.id()).outE().next();
            fail();
        }
    }

    @Test
    public void testNumericIndexDouble() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon")
                .property("color", "yellow")
                .property("type", "plant")
                .property("spots",3.14d).next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .property("spots",2.33d).next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n",3).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> twoSpots = g.V().has("spots", 2.33d).toList();
        assertEquals(1,twoSpots.size());
        List<Vertex> slt = g.V().has("spots", P.lt(4d)).toList();
        assertEquals(2,slt.size());
        List<Vertex> sgt = g.V().has("spots", P.gt(1d)).toList();
        assertEquals(2,sgt.size());
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        g.V(s1.id()).outE().drop().iterate();
        if (g.V(fruit.id()).outE().count().next() > 0) {
            Edge a = g.V(fruit.id()).outE().next();
            fail();
        }
    }


    @Test
    public void testReadWriteRemoveEdgeProperty() {
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
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        Property<Object> prop = g.V(fruit.id()).inE().next().properties("this").next();
        assertEquals("that", prop.value());
    }

    @Test
    public void testEdgeLabel() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        List<Edge> things = g.E().hasLabel("IsA").toList();
        assertEquals(2, (long) g.E().hasLabel("IsA").count().next());
        assertEquals(2, (long) g.V().outE().hasLabel("IsA").count().next());
    }

    @Test
    public void testEdgeLabel2() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        assertEquals(1, (long) g.V().has("color", "yellow").outE().count().next());
    }

    @Test
    public void g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name() {
        GraphTraversalSource g = graph.traversal();
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

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
    public void testGrateful() throws IOException {
        GraphTraversalSource g = graph.traversal();
        GraphTraversalSource g2 = TinkerFactory.createGratefulDead().traversal();
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
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
    public void noNext() {
        try {
            graph.edges(10000l).next();
            fail("Call to g.edges(10000l) should throw an exception");
        } catch (Exception ex) {
            assertThat(ex, IsInstanceOf.instanceOf(NoSuchElementException.class));
        }
    }

    @Test
    public void testTree() {
        int branchSize = 5;
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
    public void testEdgeIdScan() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        Iterator<Object> i = db.vertexBackend.getEdgeIdsFromVertex((FireflyVertex) fruit,Direction.IN);
        assertTrue(i.hasNext());
        List<Object> x = List.of(lemon.edges(Direction.OUT).next().id(), lime.edges(Direction.OUT).next().id());
        Object next = i.next();
        assertTrue(x.contains(next));
        assertTrue(x.contains(next));
    }


    public static void validateException(final Throwable expected, final Throwable actual) {
        assertThat(actual, instanceOf(expected.getClass()));
    }


    public void tryCommit(final Graph graph, final Consumer<Graph> assertFunction) {
        assertFunction.accept(graph);
        if (graph.features().graph().supportsTransactions()) {
            graph.tx().commit();
            assertFunction.accept(graph);
        }
    }

    @Test
    public void shouldOverwriteEarlierKeyValuesWithLaterKeyValuesOnAddVertexIfNoMultiProperty() {
        Vertex v = this.graph.addVertex(new Object[]{"test", "A", "test", "B", "test", "C"});
        this.tryCommit(this.graph, (graph) -> {
            Assert.assertEquals(1L, IteratorUtils.count(v.properties(new String[]{"test"})));
            Assert.assertTrue(IteratorUtils.stream(v.values(new String[]{"test"})).anyMatch((t) -> {
                return t.equals("C");
            }));
        });
    }

    public static Consumer<Graph> sngcme_getAssertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            assertEquals(expectedVertexCount, IteratorUtils.count(g.vertices()));
            assertEquals(expectedEdgeCount, IteratorUtils.count(g.edges()));
        };
    }

    public void sngcme_tryCommit(final Graph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().commit();
    }

    @Test
    public void shouldNotGetConcurrentModificationException() {
        for (int i = 0; i < 25; ++i) {
            this.graph.addVertex(new Object[]{"myId", i});
        }

        this.graph.vertices(new Object[0]).forEachRemaining((vx) -> {
            this.graph.vertices(new Object[0]).forEachRemaining((u) -> {
                vx.addEdge("knows", u, new Object[]{"myEdgeId", 12});
            });
        });
        this.tryCommit(this.graph, sngcme_getAssertVertexEdgeCounts(25, 625));
        List<Vertex> vertices = new ArrayList();
        IteratorUtils.fill(this.graph.vertices(new Object[0]), vertices);
        Iterator var2 = vertices.iterator();

        while (var2.hasNext()) {
            Vertex v = (Vertex) var2.next();
            v.remove();
            this.sngcme_tryCommit(this.graph);
        }

        this.tryCommit(this.graph, sngcme_getAssertVertexEdgeCounts(0, 0));
    }

    @Test
    public void shouldReadWriteSelfLoopingEdges() throws Exception {
        GraphSONMapper mapper = ((GraphSONIo)this.graph.io(GraphSONIo.build())).mapper().version(GraphSONVersion.V3_0).create();
        Graph source = this.graph;
        Vertex v1 = source.addVertex(new Object[0]);
        Vertex v2 = source.addVertex(new Object[0]);
        v1.addEdge("CONTROL", v2, new Object[0]);
        v1.addEdge("SELFLOOP", v1, new Object[0]);
        final HashMap<String, Object> configMap = new HashMap<>();
        graph.configuration().getKeys().forEachRemaining( k -> configMap.put(k,graph.configuration().get(String.class,k)));
        configMap.put(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "1");


        Graph targetGraph = FireflyGraph.open(new MapConfiguration(configMap));
        targetGraph.traversal().V().drop().iterate();
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            Throwable var8 = null;

            try {
                ((GraphSONIo)source.io(IoCore.graphson())).writer().mapper(mapper).create().writeGraph(os, source);
                ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
                Throwable var10 = null;

                try {
                    ((GraphSONIo)targetGraph.io(IoCore.graphson())).reader().mapper(mapper).create().readGraph(is, targetGraph);
                } catch (Throwable var35) {
                    var10 = var35;
                    throw var35;
                } finally {
                    if (is != null) {
                        if (var10 != null) {
                            try {
                                is.close();
                            } catch (Throwable var34) {
                                var10.addSuppressed(var34);
                            }
                        } else {
                            is.close();
                        }
                    }

                }
            } catch (Throwable var37) {
                var8 = var37;
                throw var37;
            } finally {
                if (os != null) {
                    if (var8 != null) {
                        try {
                            os.close();
                        } catch (Throwable var33) {
                            var8.addSuppressed(var33);
                        }
                    } else {
                        os.close();
                    }
                }

            }
        } catch (IOException var39) {
            throw new RuntimeException(var39);
        }

        Assert.assertEquals(IteratorUtils.count(source.vertices(new Object[0])), IteratorUtils.count(targetGraph.vertices(new Object[0])));
        Assert.assertEquals(IteratorUtils.count(source.edges(new Object[0])), IteratorUtils.count(targetGraph.edges(new Object[0])));
    }

}

