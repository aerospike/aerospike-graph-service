package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.FireflyConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeClientIntegration {


    @BeforeEach
    void clearData() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        ac.dropDatabase();
    }

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
    void testBasicDelete() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        Key key = new Key(c.aerospikeNamespace(), "demo", "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        ac.write(key, bin1, bin2, bin3);
        assertNotEquals(null, ac.read(key));
        ac.delete(key);
        assertNull(ac.read(key));
    }

    @Test
    void testAddRemoveIterateVertexIdList() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
        ac.writeElementId(FireflyVertex.class, 1L);
        ac.writeElementId(FireflyVertex.class, 2L);
        Iterator<Long> i = (Iterator<Long>) ac.readElementIds(FireflyVertex.class);
        assertEquals(1L, i.next());
        assertEquals(2L, i.next());
        ac.removeElementId(FireflyVertex.class, 2L);
        Iterator<Long> i2 = (Iterator<Long>) ac.readElementIds(FireflyVertex.class);
        assertEquals(1L, i2.next());
        assertFalse(i2.hasNext());
        ac.writeElementId(FireflyVertex.class, 3L);
        ac.writeElementId(FireflyVertex.class, 2L);
        Iterator<Long> i3 = (Iterator<Long>) ac.readElementIds(FireflyVertex.class);
        long last = 0L;
        while (i3.hasNext()) {
            long current = ((Number) i3.next()).longValue();
            assertTrue(current > last);
            last = current;
        }
    }
    @Test
    void testCounterOps() {
        FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        AerospikeConnection ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace());
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


}
