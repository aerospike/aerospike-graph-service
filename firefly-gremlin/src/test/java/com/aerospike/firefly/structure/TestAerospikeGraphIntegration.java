package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeGraphIntegration {

    private Configuration conf;
    private AerospikeConnection db;
    private FireflyGraph graph;


    @BeforeEach
    void openGraph() {
        conf = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        graph = new FireflyGraph(conf);
        db = graph.db;
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
        graph.variables().set("this","that");
        assertEquals("that",graph.variables().get("this").get().toString());
        assertEquals("this",graph.variables().keys().iterator().next());
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
        assertEquals(2, g.V(fruit.id()).outE().count().next());
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
        assertEquals(2, g.V(fruit.id()).outE().count().next());
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
        assertEquals(2, g.V(fruit.id()).outE().count().next());
        Property<Object> prop = g.V(fruit.id()).outE().next().properties("this").next();
        assertEquals("that", prop.value());
    }
}
