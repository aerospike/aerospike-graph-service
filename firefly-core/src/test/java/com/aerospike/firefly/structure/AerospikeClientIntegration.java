package com.aerospike.firefly.structure;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.FireflyConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AerospikeClientIntegration {
    @Test
    void testConnectToAerospike() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
    }

    @Test
    void testBasicReadWrite() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        Key key = new Key(c.aerospikeNamespace(), "demo", "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        ac.write(key, bin1, bin2, bin3);
        assertEquals(ac.read(key).getInt("age"), 32);
    }

    @Test
    void testReadWriteProperty() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = new FireflyGraph(ac, c);
        FireflyVertex vertex = new FireflyVertex(null, 1l, "label", graph);
        FireflyProperty<String> p = new FireflyProperty<>(vertex, "bkey", "b");
        ac.writeProperty(vertex, "bKey", p);
        Property readback = ac.readProperty(vertex, "bKey");
        assertEquals(p.key(), readback.key());
        assertEquals(p.value(), readback.value());
    }

    @Test
    void testReadWriteVertexProperty() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = new FireflyGraph(ac, c);
        FireflyVertex vertex = new FireflyVertex(null, graph.vertexIdManager.getNextId(graph), "label", graph);
        VertexProperty<String> p = new FireflyVertexProperty<>(graph.vertexPropertyIdManager.getNextId(graph), vertex, "a", "b");
        ac.writeVertexProperty(vertex, "aKey", List.of(p));
        Map<String, List<VertexProperty>> readBack = ac.readVertexProperties(vertex);
        List<VertexProperty> aValue = readBack.get("aKey");
        assertNotEquals(aValue, null);
        assertEquals(aValue.get(0), p);
    }

    @Test
    void testReadWriteVertex() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = new FireflyGraph(ac, c);
        ac.writeVertex(graph, 2l, "aVertexLabel");
        FireflyVertex v = ac.readVertex(graph, 2l);
        assertEquals(v.label(), "aVertexLabel");
    }

    @Test
    void testCounterOps() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = new FireflyGraph(ac, c);
        ac.zeroIdCounter("test");
        ac.incrementIdCounter("test");
        assertEquals(1, ac.getIdCounter("test"));
        ac.decrementIdCounter("test");
        assertEquals(0, ac.getIdCounter("test"));
        ac.incrementIdCounter("test");
        ac.incrementIdCounter("test");
        assertEquals(2, ac.getIdCounter("test"));
        ac.zeroIdCounter("test");
        assertEquals(0, ac.getIdCounter("test"));
    }

    @Test
    void testVertexIterator() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = new FireflyGraph(ac, c);
        List<Long> usedIds = new ArrayList<>();
        LongStream.range(0, 100).forEach(l -> {
            Long next = (Long) graph.vertexIdManager.getNextId(graph);
            usedIds.add(next);
            ac.writeVertex(graph,next , "aVertexLabel");
        });
        final AtomicLong ctr = new AtomicLong(0);
        new FireflyVertexIterator<Long>(graph, usedIds.iterator()).forEachRemaining(v -> {
            ctr.addAndGet(1);
            assertEquals("aVertexLabel", v.label());
        });
        assertEquals(100, ctr.get());
    }

    @Test
    void testGraph() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = FireflyGraph.open(ac, c);
        long id = (Long) graph.vertexIdManager.getNextId(graph);
        graph.addVertex(T.id, id).property("this", "that");
        Vertex thing = graph.vertices(id).next();
        assertEquals("that", thing.property("this").value().toString());
    }

    @Test
    void testGraphTraversal() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        FireflyGraph graph = FireflyGraph.open(ac, c);
        GraphTraversalSource g = graph.traversal();
        g.addV("puppy").property("color", "brown").next();
        Vertex thing = g.V().next();
        assertEquals("brown", g.V().hasLabel("puppy").values("color").next());
    }

}
