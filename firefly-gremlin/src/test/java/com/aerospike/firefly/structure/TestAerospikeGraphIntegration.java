package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.impl.GraphFactory;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertex;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertexProperty;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyVertexIterator;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.firefly.io.impl.relational.RelationalGraph.FIREFLY_CONFIGURATION_VARIABLE_NAME;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkResults;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeGraphIntegration extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testReadWriteRemoveGraphVariables() {
        graph.variables().set("this", "that");
        assertEquals("that", graph.variables().get("this").get().toString());
        assertEquals("this", graph.variables().keys().iterator().next());
        graph.variables().remove("this");
        assertFalse(graph.variables().keys().iterator().hasNext());
    }

    @Test
    public void testReadWriteVertexProperty() {
        final FireflyId vertexId = FireflyIdFactory.createFromManager(graph, LinkedVertex.class);
        final FireflyId vpid = FireflyIdFactory.createFromManager(graph, LinkedVertexProperty.class);
        final FireflyVertex vertex = graph.writeVertex(vertexId, "aVertexLabel", new ArrayList<>());
        final FireflyVertexProperty fireflyVertexProperty = graph.writeVertexProperty(vpid, vertex, "aKey", "aValue");

        // Try read from scratch.
        final FireflyVertex vertexRead = graph.readVertex(vertexId);
        final Iterator<VertexProperty<Object>> fireflyVertexPropertyIterator = vertexRead.readVertexProperty("aKey");
        assertTrue(fireflyVertexPropertyIterator.hasNext());
        final VertexProperty<Object> fireflyVertexPropertyRead = fireflyVertexPropertyIterator.next();
        assertEquals("aKey", fireflyVertexPropertyRead.key());
        assertEquals("aValue", fireflyVertexPropertyRead.value());
        assertEquals(vertexId.getUserId(), fireflyVertexPropertyRead.element().id());
        assertFalse(fireflyVertexPropertyIterator.hasNext());

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
        FireflyId id = FireflyIdFactory.createFromManager(graph, FireflyVertex.class);
        graph.writeVertex(id, "aVertexLabel", new ArrayList<>());
        FireflyVertex v = graph.readVertex(id);
        assertEquals(v.label(), "aVertexLabel");
    }

    @Test
    public void testVertexIterator() {
        List<Long> usedIds = new ArrayList<>();
        LongStream.range(0, 10).forEach(l -> {
            FireflyId next = FireflyIdFactory.createFromManager(graph, FireflyVertex.class);
            usedIds.add((Long) next.getUserId());
            graph.writeVertex(next, "aVertexLabel", new ArrayList<>());
        });
        final AtomicLong ctr = new AtomicLong(0);
        new FireflyVertexIterator<>(graph, usedIds.iterator()).forEachRemaining(v -> {
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
                .addE("IsA").from("b").to("a").property("a", "b").iterate();
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
                .addE("IsA").from("b").to("a").property("n", 3L).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Edge> nEdge = g.E().has("n", 3L).toList();
        assertEquals(2, nEdge.size());
        List<Edge> ltnEdge = g.E().has("n", P.lt(4L)).toList();
        assertEquals(2, ltnEdge.size());
        List<Edge> gtnEdge = g.E().has("n", P.gt(1L)).toList();
        assertEquals(2, gtnEdge.size());
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
                .property("spots", 3L).next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .property("spots", 2L).next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n", 3).iterate();
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
                .property("spots", 3).next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .property("spots", 2).next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n", 3).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> twoSpots = g.V().has("spots", 2).toList();
        assertEquals(1, twoSpots.size());
        List<Vertex> slt = g.V().has("spots", P.lt(4)).toList();
        assertEquals(2, slt.size());
        List<Vertex> sgt = g.V().has("spots", P.gt(1)).toList();
        assertEquals(2, sgt.size());
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
                .property("spots", 3.14d).next();
        Vertex lime = g.addV("lime")
                .property("color", "green")
                .property("type", "plant")
                .property("spots", 2.33d).next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("n", 3).iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> twoSpots = g.V().has("spots", 2.33d).toList();
        assertEquals(1, twoSpots.size());
        List<Vertex> slt = g.V().has("spots", P.lt(4d)).toList();
        assertEquals(2, slt.size());
        List<Vertex> sgt = g.V().has("spots", P.gt(1d)).toList();
        assertEquals(2, sgt.size());
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
        g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        final Vertex lemon = g.V().hasLabel("lemon").next();
        final Vertex lime = g.V().hasLabel("lime").next();
        List<FireflyId> i = graph.readVertex(FireflyIdFactory.createFromUser(FireflyVertex.class, fruit.id())).getEdgeIdsFromVertex(Direction.IN);
        assertFalse(i.isEmpty());
        List<Object> x = List.of(lemon.edges(Direction.OUT).next().id(), lime.edges(Direction.OUT).next().id());
        FireflyId next = i.get(0);
        assertTrue(x.contains(next.getUserId()));
        next = i.get(1);
        assertTrue(x.contains(next.getUserId()));
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
        if (StarPackedGraph.isStarPackedGraph(graph)) {
            // StarPackedGraph does not concurrent modifications.
            return;
        }
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
        GraphSONMapper mapper = ((GraphSONIo) this.graph.io(GraphSONIo.build())).mapper().version(GraphSONVersion.V3_0).create();
        Graph source = this.graph;
        Vertex v1 = source.addVertex(new Object[0]);
        Vertex v2 = source.addVertex(new Object[0]);
        v1.addEdge("CONTROL", v2, new Object[0]);
        v1.addEdge("SELFLOOP", v1, new Object[0]);
        final HashMap<String, Object> configMap = new HashMap<>();
        graph.configuration().getKeys().forEachRemaining(k -> configMap.put(k, graph.configuration().get(String.class, k)));
        configMap.put(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "1");

        Graph targetGraph = FireflyGraph.open(new MapConfiguration(configMap));
        targetGraph.traversal().V().drop().iterate();
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            Throwable var8 = null;

            try {
                ((GraphSONIo) source.io(IoCore.graphson())).writer().mapper(mapper).create().writeGraph(os, source);
                ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
                Throwable var10 = null;

                try {
                    ((GraphSONIo) targetGraph.io(IoCore.graphson())).reader().mapper(mapper).create().readGraph(is, targetGraph);
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

    private static <A> boolean internalCheckList(final List<A> expectedList, final List<A> actualList) {
        if (expectedList.size() != actualList.size()) {
            return false;
        }
        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).equals(expectedList.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static <A, B> boolean internalCheckMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() != actualList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!Objects.equals(actualList.get(i).getKey(), expectedList.get(i).getKey())) {
                return false;
            }
            if (!Objects.equals(actualList.get(i).getValue(), expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertThat(traversal.hasNext(), is(false));
        if (expectedResults.size() != results.size()) {
            LOG.error("Expected results: " + expectedResults);
            LOG.error("Actual results:   " + results);
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                assertThat("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).anyMatch(e -> internalCheckMap((Map) e, (Map) t)), is(true));
            } else if (t instanceof List) {
                assertThat("Checking list result existence: " + t, expectedResults.stream().filter(e -> e instanceof List).anyMatch(e -> internalCheckList((List) e, (List) t)), is(true));
            } else {
                assertThat("Checking result existence: " + t, expectedResults.contains(t), is(true));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1L));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1L));
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
    }

    @Test
    public void g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
        if (graph.features().vertex().supportsMultiProperties()) {
            GraphHelper.cloneElements(TinkerFactory.createTheCrew(), graph);
            GraphTraversalSource g = graph.traversal();
            Traversal<Vertex, String> traversal = g.V().local(properties("location").order().by(T.value, Order.asc).range(0, 2)).value();
            checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), traversal);
        } else {
            LOG.info("Skipping g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value because {} does not support multi-properties", graph);
        }
    }


    @Test
    public void trivialMultiProperty() {
        if (graph.features().vertex().supportsMultiProperties()) {
            GraphTraversalSource g = graph.traversal();
            Vertex z = g.addV().next();
            String[] vals = new String[]{"zontar", "zoltan"};
            g.V(z).property("name", vals[0]).next();
            g.V(z).property(VertexProperty.Cardinality.list, "name", vals[1]).next();
            assertEquals((Long) 2L, g.V(z).properties("name").count().next());
            GraphTraversal<Vertex, ? extends Property<Object>> t = g.V(z).properties("name");
            Property<Object> a = t.next();
            Property<Object> b = t.next();
            assertTrue(List.of(vals).contains((String) a.value()));
            assertTrue(List.of(vals).contains((String) b.value()));
            assertNotEquals(a.value(), b.value());
        } else {
            LOG.info("Skipping trivialMultiProperty because {} does not support multi-properties", graph);
        }
    }

    public static void assertVertexEdgeCounts(final Graph graph, final int expectedVertexCount, final int expectedEdgeCount) {
        getAssertVertexEdgeCounts(expectedVertexCount, expectedEdgeCount).accept(graph);
    }

    public static Consumer<Graph> getAssertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            assertEquals(expectedVertexCount, IteratorUtils.count(g.vertices()));
            assertEquals(expectedEdgeCount, IteratorUtils.count(g.edges()));
        };
    }

    @Test
    public void shouldRemoveMultiProperties() {
        if (graph.features().vertex().supportsMultiProperties()) {
            final Vertex v = graph.addVertex("name", "marko", "age", 34);
            v.property(VertexProperty.Cardinality.list, "name", "marko a. rodriguez");
            tryCommit(graph, x -> {
            });
            v.property(VertexProperty.Cardinality.list, "name", "marko rodriguez");
            v.property(VertexProperty.Cardinality.list, "name", "marko");
            tryCommit(graph, graph -> {
                assertEquals(5, IteratorUtils.count(v.properties()));
                assertEquals(4, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertThat(values, hasItem("marko a. rodriguez"));
                assertThat(values, hasItem("marko rodriguez"));
                assertThat(values, hasItem("marko"));
                assertVertexEdgeCounts(graph, 1, 0);
            });

            IteratorUtils.filter(v.properties(), p -> p.value().equals("marko")).forEachRemaining(VertexProperty::remove);
            List<? extends Property<Object>> l = graph.traversal().V(v).properties().toList();
            tryCommit(graph, graph -> {
                assertEquals(3, IteratorUtils.count(graph.traversal().V(v).properties()));
                assertEquals(2, IteratorUtils.count(graph.traversal().V(v).properties("name")));
                assertVertexEdgeCounts(graph, 1, 0);
            });

            v.property("age").remove();
            tryCommit(graph, graph -> {
                assertEquals(2, IteratorUtils.count(v.properties()));
                assertEquals(2, IteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(graph, 1, 0);
            });

            IteratorUtils.filter(v.properties("name"), p -> p.key().equals("name")).forEachRemaining(VertexProperty::remove);
            tryCommit(graph, graph -> {
                assertEquals(0, IteratorUtils.count(v.properties()));
                assertEquals(0, IteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(graph, 1, 0);
            });
        } else {
            LOG.info("Skipping shouldRemoveMultiProperties because {} has does not support multi-properties", graph);
        }
    }

    @Test
    public void shouldHandleListVertexPropertiesWithoutNullPropertyValues() {
        if (graph.features().vertex().supportsMultiProperties()) {
            Vertex v = this.graph.addVertex(new Object[]{"name", "marko", "age", 34});

            this.tryCommit(this.graph, (g) -> {
                Assert.assertEquals("marko", v.property("name").value());
                Assert.assertEquals("marko", v.value("name"));
                Assert.assertEquals(34, v.property("age").value());
                Assert.assertEquals(34L, (long) (Integer) v.value("age"));
                Assert.assertEquals(1L, IteratorUtils.count(v.properties(new String[]{"name"})));
                Assert.assertEquals(2L, IteratorUtils.count(v.properties(new String[0])));
                assertVertexEdgeCounts(this.graph, 1, 0);
            });
            VertexProperty<String> property = v.property(VertexProperty.Cardinality.list, "name", "marko a. rodriguez", new Object[0]);
            this.tryCommit(this.graph, (g) -> {
                Assert.assertEquals(v, property.element());
            });

            try {
                v.property("name");
                Assert.fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
            } catch (Exception var4) {
                validateException(Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"), var4);
            }

            Assert.assertTrue(IteratorUtils.list(v.values(new String[]{"name"})).contains("marko"));
            Assert.assertTrue(IteratorUtils.list(v.values(new String[]{"name"})).contains("marko a. rodriguez"));
            Assert.assertEquals(3L, IteratorUtils.count(v.properties(new String[0])));
            Assert.assertEquals(2L, IteratorUtils.count(v.properties(new String[]{"name"})));
            assertVertexEdgeCounts(this.graph, 1, 0);
            Assert.assertEquals(v, v.property(VertexProperty.Cardinality.list, "name", "mrodriguez", new Object[0]).element());
            this.tryCommit(this.graph, (g) -> {
                Assert.assertEquals(3L, IteratorUtils.count(v.properties(new String[]{"name"})));
                Assert.assertEquals(4L, IteratorUtils.count(v.properties(new String[0])));
                assertVertexEdgeCounts(this.graph, 1, 0);
            });
            v.properties(new String[]{"name"}).forEachRemaining((meta) -> {
                meta.property("counter", ((String) meta.value()).length());
            });
            this.tryCommit(this.graph, (g) -> {
                v.properties(new String[0]).forEachRemaining((meta) -> {
                    Assert.assertEquals(meta.key(), meta.label());
                    Assert.assertTrue(meta.isPresent());
                    Assert.assertEquals(v, meta.element());
                    if (meta.key().equals("age")) {
                        Assert.assertEquals(meta.value(), 34);
                        Assert.assertEquals(0L, IteratorUtils.count(meta.properties(new String[0])));
                    }

                    if (meta.key().equals("name")) {
                        Assert.assertEquals((long) ((String) meta.value()).length(), (long) (Integer) meta.value("counter"));
                        Assert.assertEquals(1L, IteratorUtils.count(meta.properties(new String[0])));
                        Assert.assertEquals(1L, (long) meta.keys().size());
                        Assert.assertTrue(meta.keys().contains("counter"));
                    }

                });
                assertVertexEdgeCounts(this.graph, 1, 0);
            });
            Assert.assertEquals(VertexProperty.empty(), v.property(VertexProperty.Cardinality.list, "name", (Object) null, new Object[0]));
            this.tryCommit(this.graph, (graph) -> {
                Assert.assertEquals(3L, IteratorUtils.count(graph.traversal().V(v).properties(new String[]{"name"})));
                Assert.assertEquals(4L, IteratorUtils.count(v.properties(new String[0])));
                assertVertexEdgeCounts(this.graph, 1, 0);
            });
            Assert.assertEquals(VertexProperty.empty(), v.property(VertexProperty.Cardinality.single, "name", (Object) null, new Object[0]));
            this.tryCommit(this.graph, (g) -> {
                Assert.assertEquals(0L, IteratorUtils.count(v.properties(new String[]{"name"})));
                Assert.assertEquals(1L, IteratorUtils.count(v.properties(new String[0])));
                assertVertexEdgeCounts(this.graph, 1, 0);
            });
        } else {
            LOG.info("Skipping shouldHandleListVertexPropertiesWithoutNullPropertyValues because graph does not support multi-properties");
        }
    }

    @Test
    public void shouldRemoveMultiPropertiesWhenVerticesAreRemoved() {
        if (graph.features().vertex().supportsMultiProperties()) {
            Vertex marko = this.graph.addVertex(new Object[]{"name", "marko", "name", "okram"});
            Vertex stephen = this.graph.addVertex(new Object[]{"name", "stephen", "name", "spmallette"});
            this.tryCommit(this.graph, (graph) -> {
                assertVertexEdgeCounts(graph, 2, 0);
                Assert.assertEquals(2L, IteratorUtils.count(marko.properties(new String[]{"name"})));
                Assert.assertEquals(2L, IteratorUtils.count(stephen.properties(new String[]{"name"})));
                Assert.assertEquals(2L, IteratorUtils.count(marko.properties(new String[0])));
                Assert.assertEquals(2L, IteratorUtils.count(stephen.properties(new String[0])));
                Assert.assertEquals(0L, IteratorUtils.count(marko.properties(new String[]{"blah"})));
                Assert.assertEquals(0L, IteratorUtils.count(stephen.properties(new String[]{"blah"})));
            });
            stephen.remove();
            this.tryCommit(this.graph, (graph) -> {
                assertVertexEdgeCounts(graph, 1, 0);
                Assert.assertEquals(2L, IteratorUtils.count(marko.properties(new String[]{"name"})));
                Assert.assertEquals(2L, IteratorUtils.count(marko.properties(new String[0])));
                Assert.assertEquals(0L, IteratorUtils.count(marko.properties(new String[]{"blah"})));
            });

            for (int i = 0; i < 100; ++i) {
                marko.property(VertexProperty.Cardinality.list, "name", "Remove-" + String.valueOf(i), new Object[0]);
            }

            this.tryCommit(this.graph, (graph) -> {
                assertVertexEdgeCounts(graph, 1, 0);
                Assert.assertEquals(102L, IteratorUtils.count(marko.properties(new String[]{"name"})));
                Assert.assertEquals(102L, IteratorUtils.count(marko.properties(new String[0])));
                Assert.assertEquals(0L, IteratorUtils.count(marko.properties(new String[]{"blah"})));
            });
            graph.traversal().V(new Object[0]).properties(new String[]{"name"}).has(T.value, P.test((a, b) -> {
                return ((String) a).startsWith((String) b);
            }, "Remove-")).forEachRemaining(Property::remove);
            this.tryCommit(this.graph, (graph) -> {
                assertVertexEdgeCounts(graph, 1, 0);
                List<VertexProperty<Object>> l = IteratorUtils.list(marko.properties(new String[]{"name"}));
                Assert.assertEquals(2L, IteratorUtils.count(marko.properties(new String[]{"name"})));
                Assert.assertEquals(2L, IteratorUtils.count(marko.properties(new String[0])));
                Assert.assertEquals(0L, IteratorUtils.count(marko.properties(new String[]{"blah"})));
            });
            marko.remove();
            this.tryCommit(this.graph, getAssertVertexEdgeCounts(0, 0));
        } else {
            LOG.info("skipping shouldRemoveMultiPropertiesWhenVerticesAreRemoved because {} does not support multi-properties", graph);
        }
    }

    @Test
    public void shouldAllowIdAssignment() {
        Vertex v = this.graph.addVertex(new Object[0]);
        Object id = Long.valueOf(123131231L);
        v.property(VertexProperty.Cardinality.single, "name", "stephen", new Object[]{T.id, id});
        this.tryCommit(this.graph, (g) -> {
            Assert.assertEquals(id, v.property("name").id());
        });
    }

    @Test
    public void shouldReturnConfigurationFromGraphVariable() {
        Object stuff = graph.readGraphVariable(FIREFLY_CONFIGURATION_VARIABLE_NAME);
        assertEquals(graph.readGraphVariable(FIREFLY_CONFIGURATION_VARIABLE_NAME), config);
    }

    @Test
    public void shouldReturnConfigurationFromMetadataVertex() {
        Vertex it = graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next();
        List<VertexProperty<Object>> props = IteratorUtils.list(graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next().properties());
        config.getKeys().forEachRemaining(key -> {
            assertEquals(graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next().property(key).value(), config.getString(key));
        });
        assertEquals(IteratorUtils.list(graph.configuration().getKeys()).size() + 2, props.size());
    }

    @Test
    public void testModelAndVersion() {
        //This initializes the metadata for version and model, if drop database is called, its cleared
        graph = GraphFactory.createGraph(db, config);

        Vertex it = graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next();
        assertEquals(graph.getBaseGraph().getDataModelName(), it.property(graph.getBaseGraph().DATA_MODEL_NAME).value());
        assertEquals(graph.getBaseGraph().getDataModelVerion().toString(), it.property(graph.getBaseGraph().DATA_MODEL_VER).value());
    }

    @Test
    public void testOperateCache() {
        RelationalVertex a = (RelationalVertex) graph.addVertex(T.label, "a");
        RelationalVertex b = (RelationalVertex) graph.addVertex(T.label, "b");
        Edge e1 = graph.traversal().addE("oneLabel").from(a).to(b).next();
        Edge e2 = graph.traversal().addE("twoLabel").from(b).to(a).next();
        List<Edge> allEdges = graph.traversal().E().toList();
        List<Edge> e1e = graph.traversal().V(a).bothE().toList();
        List<Edge> e2e = graph.traversal().V(b).bothE().toList();
        assertEquals((Long) 1L, (Long) graph.traversal().V(a).inE().count().next());
    }

    @Test
    public void shouldRemoveEdges() {
        final int vertexCount = 100;
        final int edgeCount = 200;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();
        final Random random = new Random();

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(graph.addVertex()));
        tryCommit(graph, getAssertVertexEdgeCounts(vertexCount, 0));

        IntStream.range(0, edgeCount).forEach(i -> {
            boolean created = false;
            while (!created) {
                final Vertex a = vertices.get(random.nextInt(vertices.size()));
                final Vertex b = vertices.get(random.nextInt(vertices.size()));
                if (a != b) {
                    edges.add(a.addEdge("a" + UUID.randomUUID(), b));
                    created = true;
                }
            }
        });

        tryCommit(graph, getAssertVertexEdgeCounts(vertexCount, edgeCount));

        int counter = 0;
        for (Edge e : edges) {
            counter = counter + 1;
            e.remove();

            final int currentCounter = counter;
            tryCommit(graph, getAssertVertexEdgeCounts(vertexCount, edgeCount - currentCounter));
        }
    }

}

