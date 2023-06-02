package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.GraphFactory;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.client.util.Crypto;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.firefly.structure.FireflyGraph.FIREFLY_CONFIGURATION_VARIABLE_NAME;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.identity;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        final FireflyId vertexId = graph.getIdFactory().createFromManager(graph, PackedVertex.class);
        final FireflyId vpid = graph.getIdFactory().createFromManager(graph, PackedVertexProperty.class);
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
        FireflyId id = graph.getIdFactory().createFromManager(graph, FireflyVertex.class);
        graph.writeVertex(id, "aVertexLabel", new ArrayList<>());
        FireflyVertex v = graph.readVertex(id);
        assertEquals(v.label(), "aVertexLabel");
    }

    @Test
    public void testBatchReadVertexIterator() {
        List<FireflyId> usedIds = new ArrayList<>();
        LongStream.range(0, 10).forEach(l -> {
            FireflyId next = graph.getIdFactory().createFromManager(graph, FireflyVertex.class);
            usedIds.add(next);
            graph.writeVertex(next, "aVertexLabel", new ArrayList<>());
        });
        final AtomicLong ctr = new AtomicLong(0);
        new FireflyBatchElementIterator<>(graph, usedIds.iterator(), List.of(), graph::readVertices).forEachRemaining(v -> {
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

    public Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graph.traversal().V().has("name", vertexName).toList().get(0);
    }

    public Object convertToVertexId(final Graph graph, final String vertexName) {
        return convertToVertex(graph, vertexName).id();
    }

    private final TypeReference<HashMap<String, Object>> mapTypeReference = new TypeReference<HashMap<String, Object>>() {
    };

    @Test
    @Ignore //@todo
    public void shouldSerializeTreeUncached() throws Exception {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
//        nocacheconfig.setProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase(), "false");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "nocachegraph");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase(noCacheGraph, false);

        GraphHelper.cloneElements(TinkerFactory.createModern(), noCacheGraph);

        ObjectMapper mapper = noCacheGraph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
        Tree t = noCacheGraph.traversal().V(new Object[]{this.convertToVertexId(noCacheGraph, "marko")}).out(new String[0]).properties(new String[]{"name"}).tree().next();
        String json = mapper.writeValueAsString(t);
        HashMap<String, Object> m = mapper.readValue(json, this.mapTypeReference);
        Assert.assertEquals(1L, m.size());
        Assert.assertTrue(m.containsKey(this.convertToVertex(noCacheGraph, "marko").id().toString()));
        HashMap<String, Object> branch = (HashMap) m.get(this.convertToVertexId(noCacheGraph, "marko").toString());
        Assert.assertEquals(2L, branch.size());
        Assert.assertTrue(branch.containsKey("key"));
        Assert.assertTrue(branch.containsKey("value"));
        HashMap<String, Object> branchKey = (HashMap) branch.get("key");
        Assert.assertTrue(branchKey.containsKey("id"));
        Assert.assertTrue(branchKey.containsKey("label"));
        Assert.assertTrue(branchKey.containsKey("type"));
        Assert.assertTrue(branchKey.containsKey("properties"));
        Assert.assertEquals(this.convertToVertexId(noCacheGraph, "marko").toString(), branchKey.get("id").toString());
        Assert.assertEquals("person", branchKey.get("label"));
        Assert.assertEquals("vertex", branchKey.get("type"));
        HashMap<String, List<HashMap<String, Object>>> branchKeyProps = (HashMap) branchKey.get("properties");
        Assert.assertEquals("marko", ((HashMap) ((List) branchKeyProps.get("name")).get(0)).get("value"));
        Assert.assertEquals(29, ((HashMap) ((List) branchKeyProps.get("age")).get(0)).get("value"));
        HashMap<String, Object> branchValue = (HashMap) branch.get("value");
        Assert.assertEquals(3L, branchValue.size());
        Assert.assertTrue(branchValue.containsKey(this.convertToVertexId(noCacheGraph, "vadas").toString()));
        Assert.assertTrue(branchValue.containsKey(this.convertToVertexId(noCacheGraph, "lop").toString()));
        Assert.assertTrue(branchValue.containsKey(this.convertToVertexId(noCacheGraph, "josh").toString()));
        HashMap<String, HashMap<String, Object>> branch2 = (HashMap) branchValue.get(this.convertToVertexId(noCacheGraph, "vadas").toString());
        Assert.assertTrue(branch2.containsKey("key"));
        Assert.assertTrue(branch2.containsKey("value"));
        Map.Entry entry = (Map.Entry) ((HashMap) branch2.get("value")).entrySet().iterator().next();
        HashMap<String, HashMap<String, Object>> branch2Prop = (HashMap) entry.getValue();
        Assert.assertTrue(branch2Prop.get("key").containsKey("id"));
        Assert.assertTrue(branch2Prop.get("key").containsKey("value"));
        Assert.assertTrue(branch2Prop.get("key").containsKey("label"));
        Assert.assertEquals("name", branch2Prop.get("key").get("label"));
        Assert.assertEquals("vadas", branch2Prop.get("key").get("value"));
        Assert.assertEquals(entry.getKey().toString(), branch2Prop.get("key").get("id").toString());
    }

    @Test
    public void basic_edge_cache_nocache() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "nocachegraph");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);

        Configuration cacheConfig = ConfigurationUtils.cloneConfiguration(config);
        cacheConfig.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "true");
        cacheConfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "cg");
        cacheConfig.setProperty(Graph.GRAPH, "cachegraph");

        FireflyGraph cacheGraph = FireflyGraph.open(cacheConfig);
        noCacheGraph.getBaseGraph().dropDatabase(noCacheGraph, false);
        cacheGraph.getBaseGraph().dropDatabase(cacheGraph, false);

        Vertex ncgVa = noCacheGraph.traversal().addV().next();
        Vertex ncgVb = noCacheGraph.traversal().addV().next();
        noCacheGraph.traversal().addE("knows").from(ncgVa).to(ncgVb).next();

        Vertex cgVa = cacheGraph.traversal().addV().next();
        Vertex cgVb = cacheGraph.traversal().addV().next();
        cacheGraph.traversal().addE("knows").from(cgVa).to(cgVb).next();

        Edge ncoe = noCacheGraph.traversal().V(ncgVa).outE().next();
        Edge coe = cacheGraph.traversal().V(cgVa).outE().next();
        LOG.info("noCacheGraph edge: {}", ncoe);
        LOG.info("cacheGraph edge: {}", coe);

        Edge ncie = noCacheGraph.traversal().V(ncgVb).inE().next();
        Edge cie = cacheGraph.traversal().V(cgVb).inE().next();
        LOG.info("noCacheGraph edge: {}", ncie);
        LOG.info("cacheGraph edge: {}", cie);
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
            final byte[] data = new byte[16];
            new Random().nextBytes(data);
            graph.edges(data).next();
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
        assertEquals(0L, FireflyCloseableIteratorUtils.count(start.edges(Direction.IN)));
        assertEquals(branchSize, FireflyCloseableIteratorUtils.count(start.edges(Direction.OUT)));
        Iterator var9 = FireflyCloseableIteratorUtils.list(start.edges(Direction.OUT)).iterator();

        while (var9.hasNext()) {
            Edge a = (Edge) var9.next();
            Assert.assertEquals("test1", a.label());

            Assert.assertEquals(branchSize, FireflyCloseableIteratorUtils.count(a.inVertex().vertices(Direction.OUT)));
            Assert.assertEquals(1L, FireflyCloseableIteratorUtils.count(a.inVertex().vertices(Direction.IN)));
            Iterator var12 = FireflyCloseableIteratorUtils.list(a.inVertex().edges(Direction.OUT)).iterator();

            while (var12.hasNext()) {
                Edge b = (Edge) var12.next();
                Assert.assertEquals("test2", b.label());
                Assert.assertEquals(branchSize, FireflyCloseableIteratorUtils.count(b.inVertex().vertices(Direction.OUT)));
                Assert.assertEquals(1L, FireflyCloseableIteratorUtils.count(b.inVertex().vertices(Direction.IN)));
                Iterator var14 = FireflyCloseableIteratorUtils.list(b.inVertex().edges(Direction.OUT)).iterator();

                while (var14.hasNext()) {
                    Edge c = (Edge) var14.next();
                    Assert.assertEquals("test3", c.label());
                    Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(c.inVertex().vertices(Direction.OUT)));
                    Assert.assertEquals(1L, FireflyCloseableIteratorUtils.count(c.inVertex().vertices(Direction.IN)));
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
        List<FireflyId> i = graph.readVertex(graph.getIdFactory().createFromUser(FireflyVertex.class, fruit.id())).getEdgeIdsFromVertex(Direction.IN);
        assertFalse(i.isEmpty());
        List<Object> x = List.of(lemon.edges(Direction.OUT).next().id(), lime.edges(Direction.OUT).next().id());
        FireflyId next = i.get(0);
        assertTrue(x.contains(Crypto.encodeBase64(((ByteBuffer) next.getUserId()).array())));
        next = i.get(1);
        assertTrue(x.contains(Crypto.encodeBase64(((ByteBuffer) next.getUserId()).array())));
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
            assertEquals(expectedVertexCount, FireflyCloseableIteratorUtils.count(g.vertices()));
            assertEquals(expectedEdgeCount, FireflyCloseableIteratorUtils.count(g.edges()));
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
            graph.addVertex("myId", i);
        }

        graph.vertices(new Object[0]).forEachRemaining((vx) -> {
            graph.vertices(new Object[0]).forEachRemaining((u) -> {
                vx.addEdge("knows", u, "myEdgeId", 12);
            });
        });
        this.tryCommit(graph, sngcme_getAssertVertexEdgeCounts(25, 625));
        List<Vertex> vertices = new ArrayList();
        FireflyCloseableIteratorUtils.fill(graph.vertices(), vertices);
        Iterator var2 = vertices.iterator();

        while (var2.hasNext()) {
            Vertex v = (Vertex) var2.next();
            v.remove();
            this.sngcme_tryCommit(graph);
        }

        this.tryCommit(graph, sngcme_getAssertVertexEdgeCounts(0, 0));
    }

    @Test
    public void shouldReadWriteSelfLoopingEdges() throws Exception {
        GraphSONMapper mapper = graph.io(GraphSONIo.build()).mapper().version(GraphSONVersion.V3_0).create();
        Graph source = graph;
        Vertex v1 = source.addVertex();
        Vertex v2 = source.addVertex();
        v1.addEdge("CONTROL", v2);
        v1.addEdge("SELFLOOP", v1);
        final HashMap<String, Object> configMap = new HashMap<>();
        graph.configuration().getKeys().forEachRemaining(k -> configMap.put(k, graph.configuration().get(String.class, k)));
        configMap.put(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "1");

        Graph targetGraph = FireflyGraph.open(new MapConfiguration(configMap));
        targetGraph.traversal().V().drop().iterate();
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            Throwable var8 = null;

            try {
                source.io(IoCore.graphson()).writer().mapper(mapper).create().writeGraph(os, source);
                ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
                Throwable var10 = null;

                try {
                    targetGraph.io(IoCore.graphson()).reader().mapper(mapper).create().readGraph(is, targetGraph);
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

        Assert.assertEquals(FireflyCloseableIteratorUtils.count(source.vertices()), FireflyCloseableIteratorUtils.count(targetGraph.vertices()));
        Assert.assertEquals(FireflyCloseableIteratorUtils.count(source.edges()), FireflyCloseableIteratorUtils.count(targetGraph.edges()));
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
                try {
                    assertThat("Checking result existence: " + t, expectedResults.contains(t), is(true));
                } catch (Exception e) {
                    throw e;
                }
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
            assertEquals(expectedVertexCount, FireflyCloseableIteratorUtils.count(g.vertices()));
            assertEquals(expectedEdgeCount, FireflyCloseableIteratorUtils.count(g.edges()));
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
                assertEquals(5, FireflyCloseableIteratorUtils.count(v.properties()));
                assertEquals(4, FireflyCloseableIteratorUtils.count(v.properties("name")));
                final List<String> values = FireflyCloseableIteratorUtils.list(v.values("name"));
                assertThat(values, hasItem("marko a. rodriguez"));
                assertThat(values, hasItem("marko rodriguez"));
                assertThat(values, hasItem("marko"));
                assertVertexEdgeCounts(graph, 1, 0);
            });

            FireflyCloseableIteratorUtils.filter(v.properties(), p -> p.value().equals("marko")).forEachRemaining(VertexProperty::remove);
            List<? extends Property<Object>> l = graph.traversal().V(v).properties().toList();
            tryCommit(graph, graph -> {
                assertEquals(3, FireflyCloseableIteratorUtils.count(graph.traversal().V(v).properties()));
                assertEquals(2, FireflyCloseableIteratorUtils.count(graph.traversal().V(v).properties("name")));
                assertVertexEdgeCounts(graph, 1, 0);
            });

            v.property("age").remove();
            tryCommit(graph, graph -> {
                assertEquals(2, FireflyCloseableIteratorUtils.count(v.properties()));
                assertEquals(2, FireflyCloseableIteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(graph, 1, 0);
            });

            FireflyCloseableIteratorUtils.filter(v.properties("name"), p -> p.key().equals("name")).forEachRemaining(VertexProperty::remove);
            tryCommit(graph, graph -> {
                assertEquals(0, FireflyCloseableIteratorUtils.count(v.properties()));
                assertEquals(0, FireflyCloseableIteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(graph, 1, 0);
            });
        } else {
            LOG.info("Skipping shouldRemoveMultiProperties because {} has does not support multi-properties", graph);
        }
    }

    @Test
    public void shouldHandleListVertexPropertiesWithoutNullPropertyValues() {
        if (graph.features().vertex().supportsMultiProperties()) {
            Vertex v = graph.addVertex("name", "marko", "age", 34);

            this.tryCommit(graph, (g) -> {
                Assert.assertEquals("marko", v.property("name").value());
                Assert.assertEquals("marko", v.value("name"));
                Assert.assertEquals(34, v.property("age").value());
                Assert.assertEquals(34L, (long) (Integer) v.value("age"));
                Assert.assertEquals(1L, FireflyCloseableIteratorUtils.count(v.properties("name")));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(graph, 1, 0);
            });
            VertexProperty<String> property = v.property(VertexProperty.Cardinality.list, "name", "marko a. rodriguez");
            this.tryCommit(graph, (g) -> {
                Assert.assertEquals(v, property.element());
            });

            try {
                v.property("name");
                Assert.fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
            } catch (Exception var4) {
                validateException(Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"), var4);
            }

            Assert.assertTrue(FireflyCloseableIteratorUtils.list(v.values("name")).contains("marko"));
            Assert.assertTrue(FireflyCloseableIteratorUtils.list(v.values("name")).contains("marko a. rodriguez"));
            Assert.assertEquals(3L, FireflyCloseableIteratorUtils.count(v.properties()));
            Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(v.properties("name")));
            assertVertexEdgeCounts(graph, 1, 0);
            Assert.assertEquals(v, v.property(VertexProperty.Cardinality.list, "name", "mrodriguez", new Object[0]).element());
            this.tryCommit(graph, (g) -> {
                Assert.assertEquals(3L, FireflyCloseableIteratorUtils.count(v.properties("name")));
                Assert.assertEquals(4L, FireflyCloseableIteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(graph, 1, 0);
            });
            v.properties(new String[]{"name"}).forEachRemaining((meta) -> {
                meta.property("counter", ((String) meta.value()).length());
            });
            this.tryCommit(graph, (g) -> {
                v.properties(new String[0]).forEachRemaining((meta) -> {
                    Assert.assertEquals(meta.key(), meta.label());
                    Assert.assertTrue(meta.isPresent());
                    Assert.assertEquals(v, meta.element());
                    if (meta.key().equals("age")) {
                        Assert.assertEquals(meta.value(), 34);
                        Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(meta.properties()));
                    }

                    if (meta.key().equals("name")) {
                        Assert.assertEquals(((String) meta.value()).length(), (long) (Integer) meta.value("counter"));
                        Assert.assertEquals(1L, FireflyCloseableIteratorUtils.count(meta.properties()));
                        Assert.assertEquals(1L, meta.keys().size());
                        Assert.assertTrue(meta.keys().contains("counter"));
                    }

                });
                assertVertexEdgeCounts(graph, 1, 0);
            });
            Assert.assertEquals(VertexProperty.empty(), v.property(VertexProperty.Cardinality.list, "name", null));
            this.tryCommit(graph, (graph) -> {
                Assert.assertEquals(3L, FireflyCloseableIteratorUtils.count(graph.traversal().V(v).properties("name")));
                Assert.assertEquals(4L, FireflyCloseableIteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(AbstractFireflySuite.graph, 1, 0);
            });
            Assert.assertEquals(VertexProperty.empty(), v.property(VertexProperty.Cardinality.single, "name", null));
            this.tryCommit(graph, (g) -> {
                Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(v.properties("name")));
                Assert.assertEquals(1L, FireflyCloseableIteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(graph, 1, 0);
            });
        } else {
            LOG.info("Skipping shouldHandleListVertexPropertiesWithoutNullPropertyValues because graph does not support multi-properties");
        }
    }

    @Test
    public void shouldRemoveMultiPropertiesWhenVerticesAreRemoved() {
        if (graph.features().vertex().supportsMultiProperties()) {
            final Vertex marko = graph.addVertex("name", "marko", "name", "okram");
            final Vertex stephen = graph.addVertex("name", "stephen", "name", "spmallette");
            this.tryCommit(graph, (graph) -> {
                assertVertexEdgeCounts(graph, 2, 0);
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(marko.properties("name")));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(stephen.properties("name")));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(marko.properties()));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(stephen.properties()));
                Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(marko.properties("blah")));
                Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(stephen.properties("blah")));
            });
            stephen.remove();
            this.tryCommit(graph, (graph) -> {
                assertVertexEdgeCounts(graph, 1, 0);
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(marko.properties("name")));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(marko.properties()));
                Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(marko.properties("blah")));
            });

            for (int i = 0; i < 100; ++i) {
                marko.property(VertexProperty.Cardinality.list, "name", "Remove-" + i);
            }

            this.tryCommit(graph, (graph) -> {
                assertVertexEdgeCounts(graph, 1, 0);
                Assert.assertEquals(102L, FireflyCloseableIteratorUtils.count(marko.properties("name")));
                Assert.assertEquals(102L, FireflyCloseableIteratorUtils.count(marko.properties()));
                Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(marko.properties("blah")));
            });
            graph.traversal().V(new Object[0]).properties(new String[]{"name"}).has(T.value, P.test((a, b) -> {
                return ((String) a).startsWith((String) b);
            }, "Remove-")).forEachRemaining(Property::remove);

            final Vertex alsoMarko;
            if (graph.getDataModel().equals("linked")) {
                // If using linked model, vertex properties are cached so must get a new instance of the vertex since
                // the updating traversal did not utilize the existing cached vertex used for test assertions
                alsoMarko = graph.traversal().V().has("name", "marko").next();
            } else {
                alsoMarko = marko;
            }

            this.tryCommit(graph, (graph) -> {
                assertVertexEdgeCounts(graph, 1, 0);
                List<VertexProperty<Object>> l = FireflyCloseableIteratorUtils.list(alsoMarko.properties("name"));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(alsoMarko.properties("name")));
                Assert.assertEquals(2L, FireflyCloseableIteratorUtils.count(alsoMarko.properties()));
                Assert.assertEquals(0L, FireflyCloseableIteratorUtils.count(alsoMarko.properties("blah")));
            });
            marko.remove();
            this.tryCommit(graph, getAssertVertexEdgeCounts(0, 0));
        } else {
            LOG.info("skipping shouldRemoveMultiPropertiesWhenVerticesAreRemoved because {} does not support multi-properties", graph);
        }
    }

    @Test
    public void shouldNotAllowIdAssignment() {
        Vertex v = graph.addVertex();
        Object id = Long.valueOf(123131231L);
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> v.property(VertexProperty.Cardinality.single, "name", "stephen",
                        new Object[]{T.id, id}));
    }

    @Test
    public void shouldReturnConfigurationFromGraphVariable() {
        Object stuff = graph.readGraphVariable(FIREFLY_CONFIGURATION_VARIABLE_NAME);
        assertEquals(graph.readGraphVariable(FIREFLY_CONFIGURATION_VARIABLE_NAME), config);
    }

    @Test
    public void shouldReturnConfigurationFromMetadataVertex() {
        Vertex it = graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next();
        List<VertexProperty<Object>> props = FireflyCloseableIteratorUtils.list(graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next().properties());
        config.getKeys().forEachRemaining(key -> {
            assertEquals(graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next().property(key).value(), config.getString(key));
        });
        assertEquals(FireflyCloseableIteratorUtils.list(graph.configuration().getKeys()).size() + 2, props.size());
    }

    @Test
    public void testModelAndVersion() {
        //This initializes the metadata for version and model, if drop database is called, its cleared
        graph = GraphFactory.createGraph(db, config);

        Vertex it = graph.traversal().V(FIREFLY_CONFIGURATION_VARIABLE_NAME).next();
        assertEquals(graph.getBaseGraph().getDataModelName(), it.property(AerospikeConnection.DATA_MODEL_NAME).value());
        assertEquals(graph.getBaseGraph().getDataModelVersion().toString(), it.property(AerospikeConnection.DATA_MODEL_VER).value());
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
        assertEquals((Long) 1L, graph.traversal().V(a).inE().count().next());
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

    @Test
    public void testVertexExists() {
        Vertex v = graph.addVertex();
        assertTrue(graph.vertices(v.id()).hasNext());
        v.remove();
        assertFalse(graph.vertices(v.id()).hasNext());
        List<Long> x = List.of(1L, 2L, 3L, 4L);
        x.stream().map(it -> graph.addVertex(T.id, it)).forEach(v1 -> assertTrue(graph.vertices(v1.id()).hasNext()));
        Iterator<Vertex> iter = graph.vertices(8L, 9L, 1L, 2L, 3L, 4L);
        int ctr = 0;
        while (iter.hasNext()) {
            iter.next();
            ctr++;
        }
        assertEquals(x.size(), ctr);
        assertFalse(graph.vertices(22, 35, 16, 92).hasNext());
    }

    @Test
    public void testEdgeExists() {
        Vertex va = graph.addVertex();
        Vertex vb = graph.addVertex();
        Vertex vc = graph.addVertex();
        Edge eab = va.addEdge("test", vb);
        Edge eac = va.addEdge("test", vc);
        Edge ebc = vb.addEdge("test", vc);
        assertTrue(graph.edges(eab.id()).hasNext());
        Iterator<Edge> iterAllKnown = graph.edges(eab.id(), eac.id(), ebc.id());
        assertTrue(iterAllKnown.hasNext());
        iterAllKnown.next();
        assertTrue(iterAllKnown.hasNext());
        iterAllKnown.next();
        assertTrue(iterAllKnown.hasNext());
        iterAllKnown.next();
        assertFalse(iterAllKnown.hasNext());
        eab.remove();
        assertFalse(graph.edges(eab.id()).hasNext());
        final byte[] buff = new byte[16];
        new Random().nextBytes(buff);
        Iterator<Edge> iter = graph.edges(eac.id(), ebc.id(), eab.id(), buff);
        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testEdgeExistsBatch() {
        Vertex va = graph.addVertex();
        Vertex vb = graph.addVertex();
        Vertex vc = graph.addVertex();
        Edge eab = va.addEdge("test", vb);
        Edge eac = va.addEdge("test", vc);
        Edge ebc = vb.addEdge("test", vc);
        List<Vertex> vl = new ArrayList<>();
        List<Edge> el = new ArrayList<>();
        assertEquals(3, FireflyCloseableIteratorUtils.count(graph.vertices(va.id(), vb.id(), vc.id())));
        assertEquals(3, FireflyCloseableIteratorUtils.count(graph.edges(eab.id(), eac.id(), ebc.id())));
        IntStream.range(0, db.AEROSPIKE_BATCH_READ_SIZE + 3).forEach(i -> {
            Vertex v = graph.addVertex();
            vl.add(v);
            el.add(va.addEdge("test", v));
        });
        Object[] vertexIdArray = new Vertex[vl.size()];
        vl.toArray(vertexIdArray);
        assertEquals(db.AEROSPIKE_BATCH_READ_SIZE + 3, FireflyCloseableIteratorUtils.count(graph.vertices(vertexIdArray)));
        Object[] edgeIdArray = new Edge[el.size()];
        el.toArray(edgeIdArray);
        assertEquals(db.AEROSPIKE_BATCH_READ_SIZE + 3, FireflyCloseableIteratorUtils.count(graph.edges(edgeIdArray)));
    }

    @Test
    public void shouldAllowStringID() {
        String id = "aSlimySalamander";
        Vertex v = graph.addVertex(T.id, id);
        assertEquals(id, v.id());
    }

    @Test
    public void printConfig() {
        List.of(ConfigurationHelper.Keys.class.getDeclaredFields()).forEach(field -> {
            try {
                String value = ConfigurationHelper.getOrDefault(field.getName(), config);
                System.out.printf("%s=%s%n\n", field.getName().toLowerCase(), value);
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
            }
        });
    }

    final String id1 = "< ID_1 >";
    final String entity2 = "< ENTITY_2 >";

    public List<Vertex> executeanonymizedcustomer(final GraphTraversalSource g) {
        return g.V(id1).
                fold().coalesce(
                        __.unfold(),
                        __.addV("entity_link").
                                property(T.id, id1).
                                property("type", "id").
                                property("opt_ind", "0").
                                property("first_seen", "< epoch_timestamp >").
                                property("last_seen", "< epoch_timestamp >"))
                .sideEffect(
                        __.V(id1).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", "< epoch_timestamp >")))
                .sideEffect(
                        __.V(entity2).fold().
                                coalesce(
                                        __.unfold(),
                                        __.addV("address").
                                                property(T.id, entity2).
                                                property("type", "entity").
                                                property("first_seen", "< epoch_timestamp >")))
                .sideEffect(
                        __.V(entity2).
                                coalesce(
                                        __.properties("internal dataset").
                                                drop()))
                .sideEffect(
                        __.V(entity2).
                                inE("identifies").
                                outV().hasId(id1).fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", "< epoch_timestamp >").
                                                from(__.V(id1)).to(__.V(entity2))))
                .sideEffect(
                        __.V(entity2).
                                inE("identifies").as("edge").outV().hasId(id1).select("edge").properties("internal dataset").drop()).toList();
    }

    List<Vertex> executeMarko(final GraphTraversalSource g) {
        return g.V(id1).
                fold().coalesce(
                        __.unfold(),
                        __.addV("entity_link").
                                property(T.id, id1).
                                property("type", "id").
                                property("opt_ind", "0").
                                property("first_seen", "< epoch_timestamp >").
                                property("last_seen", "< epoch_timestamp >")).
                sideEffect(
                        __.V(id1).coalesce(
                                __.properties("internal dataset").drop(),
                                __.property("last_seen", "< epoch_timestamp >"))).
                sideEffect(
                        __.V(entity2).
                                sideEffect(__.fold().coalesce(
                                        __.unfold(),
                                        __.addV("address").property(T.id, entity2).
                                                property("type", "entity").
                                                property("first_seen", "< epoch_timestamp >"))).
                                sideEffect(__.properties("internal dataset").drop()).
                                sideEffect(
                                        __.in("identifies").hasId(id1).
                                                fold().coalesce(
                                                        __.unfold(),
                                                        __.addE("identifies").property("first_seen", "< epoch_timestamp >").
                                                                from(__.V(id1)).
                                                                to(__.V(entity2)))).
                                sideEffect(
                                        __.inE("identifies").
                                                where(__.outV().hasId(id1)).
                                                properties("internal dataset").
                                                drop())).toList();
    }

    List<Vertex> executeLyndon(final GraphTraversalSource g) {
        return g.V(id1).
                fold().coalesce(
                        __.unfold(),
                        __.addV("entity_link").
                                property(T.id, id1).
                                property("type", "id").
                                property("opt_ind", "0").
                                property("first_seen", "< epoch_timestamp >").
                                property("last_seen", "< epoch_timestamp >")).
                sideEffect(
                        __.V(id1).coalesce(
                                __.properties("internal dataset").drop(),
                                __.property("last_seen", "< epoch_timestamp >"))).
                sideEffect(
                        __.V(entity2).fold().
                                coalesce(
                                        __.unfold(),
                                        __.addV("address").property(T.id, entity2).
                                                property("type", "entity").
                                                property("first_seen", "< epoch_timestamp >")).
                                sideEffect(__.properties("internal dataset").drop()).
                                sideEffect(
                                        __.in("identifies").hasId(id1).
                                                fold().coalesce(
                                                        __.unfold(),
                                                        __.addE("identifies").property("first_seen", "< epoch_timestamp >").
                                                                from(__.V(id1)).
                                                                to(__.V(entity2)))).
                                sideEffect(
                                        __.inE("identifies").
                                                where(__.outV().hasId(id1)).
                                                properties("internal dataset").
                                                drop())).toList();
    }

    @Test
    public void testanonymizedcustomerQuery() {
        GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        // g.addV("address").property(T.id, entity2).iterate();
        List<Vertex> vertices = executeanonymizedcustomer(g);
        List<Vertex> gV = g.V().toList();
        System.out.println("done");
    }

    @Test
    public void testQueries() {
        GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        List<Vertex> verticesMarko = executeMarko(g);
        List<Vertex> gVMarko = g.V().toList();
        g.V().drop().iterate();
        List<Vertex> verticesanonymizedcustomer = executeanonymizedcustomer(g);
        List<Vertex> gVanonymizedcustomer = g.V().toList();
        g.V().drop().iterate();
        List<Vertex> verticesLyndon = executeLyndon(g);
        List<Vertex> gVLyndon = g.V().toList();
        System.out.println("done");
    }


    @Test
    public void benchmark_g_E_get_props() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        final GraphTraversalSource g = graph.traversal();
        final List<Object> modernVIDList = IteratorUtils.list(g.V().id());
        final Random random = new Random();
        final Object id = modernVIDList.get(random.nextInt(modernVIDList.size()));
        //Retrieve all properties from edges attached to vertex
        final Map<String, Object> props = g.V(id).
                bothE().propertyMap().next();
    }


    @Test
    public void benchmark_g_E_get_one_prop() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        final GraphTraversalSource g = graph.traversal();
        final List<Object> modernVIDList = IteratorUtils.list(g.V().id());
        final Random random = new Random();
        final Object id = modernVIDList.get(random.nextInt(modernVIDList.size()));
        //Retrieve all properties from edges attached to vertex
        final Edge it = g.V(id).bothE().next();
        if (it.properties().hasNext()) {
            it.properties().next();
        }
    }

    private String randomString(int len) {
        final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        final Random rnd = new Random();
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }


    @Test
    public void benchmark_g_E_addProp() {
        //Add a new property to the edge pack
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        final GraphTraversalSource g = graph.traversal();
        final List<Object> modernVIDList = IteratorUtils.list(g.V().id());
        final Random random = new Random();
        final Object id = modernVIDList.get(random.nextInt(modernVIDList.size()));
        final Object eid = g.V(id).bothE().next().id();
        final Long initialCount = g.E(eid).properties().count().next();
        g.E(eid).property(randomString(10), randomString(10)).next();
        final Long finalCount = g.E(eid).properties().count().next();
        assertEquals((long) initialCount + 1, (long) finalCount);
    }


}

