package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.io.AerospikeConnection.*;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeClientIntegration {

    private Configuration c;
    private AerospikeConnection db;

    @BeforeEach
    void setup() {
        c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        db = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(c),
                ConfigurationHelper.aerospikePort(c),
                ConfigurationHelper.aerospikeNamespace(c));
        db.dropDatabase();
    }

    @AfterEach
    void cleanup() {
        db.dropDatabase();
        db.close();
    }

    @Test
    void testConnectToAerospike() {
        Configuration test_c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection test_db = AerospikeConnection.connect(
                ConfigurationHelper.aerospikeHost(test_c),
                ConfigurationHelper.aerospikePort(test_c),
                ConfigurationHelper.aerospikeNamespace(test_c));
        test_db.close();
    }

    @Test
    void testBasicReadWrite() {
        Key key = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        db.write(key, bin1, bin2, bin3);
        assertEquals(db.read(key).getInt("age"), 32);
    }

    @Test
    void testBasicDelete() {
        Key key = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        db.write(key, bin1, bin2, bin3);
        assertNotEquals(null, db.read(key));
        db.delete(key);
        assertNull(db.read(key));
    }

    @Test
    void testDropDatabase() throws InterruptedException {
        Key key = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        db.write(key, bin1, bin2, bin3);
        assertNotEquals(null, db.read(key));
        db.dropDatabase();
        assertNull(db.read(key));
    }

//    @Test
//    void testAddRemoveIterateVertexIdList() {
//        db.writeElementId(FireflyVertex.class, 1L);
//        db.writeElementId(FireflyVertex.class, 2L);
//        Iterator<Long> i = (Iterator<Long>) db.readElementIds(FireflyVertex.class);
//        assertEquals(1L, i.next());
//        assertEquals(2L, i.next());
//        db.removeElementId(FireflyVertex.class, 2L);
//        Iterator<Long> i2 = (Iterator<Long>) db.readElementIds(FireflyVertex.class);
//        assertEquals(1L, i2.next());
//        assertFalse(i2.hasNext());
//        db.writeElementId(FireflyVertex.class, 3L);
//        db.writeElementId(FireflyVertex.class, 2L);
//        Iterator<Long> i3 = (Iterator<Long>) db.readElementIds(FireflyVertex.class);
//        long last = 0L;
//        while (i3.hasNext()) {
//            long current = ((Number) i3.next()).longValue();
//            assertTrue(current > last);
//            last = current;
//        }
//    }

    @Test
    void testCounterOps() {
        db.zeroIdCounter(GLOBAL);
        db.incrementAndGetIdCounter(GLOBAL);
        assertEquals(1, db.getIdCounter(GLOBAL));
        db.decrementIdCounter(GLOBAL);
        assertEquals(0, db.getIdCounter(GLOBAL));
        db.incrementAndGetIdCounter(GLOBAL);
        db.incrementAndGetIdCounter(GLOBAL);
        assertEquals(2, db.getIdCounter(GLOBAL));
        db.zeroIdCounter(GLOBAL);
        assertEquals(0, db.getIdCounter(GLOBAL));
    }

    @Test
    void testScanVertexIds() {
        FireflyGraph graph = FireflyGraph.open(c);
        ArrayList<Long> ids = new ArrayList<>() {{
            add(0L);
            add(1L);
        }};
        db.writeVertex(graph, ids.get(0), "a");
        db.writeVertex(graph, ids.get(1), "b");
        Iterator<Long> i = db.scanAllIdsInSet(VERTEX_AERO_SET);
        assertTrue(i.hasNext());
        Long a = i.next();
        assertTrue(ids.contains(a));
        Long b = i.next();
        assertTrue(ids.contains(b));
    }

    @Test
    void testScanEdgeIds() {
        FireflyGraph graph = FireflyGraph.open(c);
        ArrayList<Long> vertexIds = new ArrayList<>() {{
            add(0L);
            add(1L);
        }};
        ArrayList<Long> edgeIds = new ArrayList<>() {{
            add(2L);
            add(3L);
        }};
        db.writeVertex(graph, vertexIds.get(0), "a");
        FireflyVertex va = db.readVertex(graph, vertexIds.get(0));
        db.writeVertex(graph, vertexIds.get(1), "b");
        FireflyVertex vb = db.readVertex(graph, vertexIds.get(1));
        Iterator<Long> i = db.scanAllIdsInSet(VERTEX_AERO_SET);
        assertTrue(i.hasNext());
        Long a = i.next();
        assertTrue(vertexIds.contains(a));
        Long b = i.next();
        assertTrue(vertexIds.contains(b));

        db.writeEdge(graph,edgeIds.get(0),"anything",va,vb,new Object[]{});
        db.writeEdge(graph,edgeIds.get(1),"anything",vb,va,new Object[]{});

        Iterator<Long> ie = db.scanAllIdsInSet(EDGE_AERO_SET);
        assertTrue(ie.hasNext());
        Long ae = ie.next();
        assertTrue(edgeIds.contains(ae));
        Long be = ie.next();
        assertTrue(edgeIds.contains(be));
    }
    @Test
    void testScanQuery() {
        long key1value = 1L;
        Key key1 = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, key1value);
        Bin key1Bin = new Bin(KEY, key1value);
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        db.write(key1, key1Bin, bin1, bin2, bin3);
        long key2value = 2L;
        Key key2 = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, key2value);
        Bin key2Bin = new Bin(KEY, key2value);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        Bin bin23 = new Bin("greeting", "Hello World!");
        db.write(key2, key2Bin, bin21, bin22, bin23);
        Iterator<Map.Entry<Key, Record>> i = db.scanAllRecordsInSet(TEST_SET);
        HashMap<Key, Record> results = new HashMap<>();
        i.forEachRemaining(entry -> {
            results.put(entry.getKey(), entry.getValue());
        });
        assertEquals(results.get(key1).getValue("name"), bin1.value.getObject());
        assertEquals(results.get(key2).getValue("name"), bin21.value.getObject());
    }


}
