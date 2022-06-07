package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeClientIntegration {

    private Configuration configuration;
    private AerospikeConnection db;

    @BeforeEach
    void setup() {
        configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        db = AerospikeConnection.connect(configuration);
        db.dropDatabase();
    }

    @AfterEach
    void cleanup() {
        db.dropDatabase();
        db.close();
    }

    @Test
    void testConnectToAerospike() {
        Configuration configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection test_db = AerospikeConnection.connect(configuration);
        test_db.close();
    }

    @Test
    void testBasicReadWrite() {
        Object id = "foo";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, id)).record.getInt("age"), 32);
    }

    @Test
    void testBasicReadWriteGetValueOfKey() {
        Object id = "foo";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");


        Policy rp = new Policy();
        rp.sendKey = true;
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
        Key key = db.scanAllRecordsInSet(db.TEST_SET).next().getKey();
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, id)).record.getInt("age"), 32);
    }

    @Test
    void testBasicDelete() {
        Object id = "foo";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
        assertNotEquals(null, db.read(FireflyRecord.getKey(db.namespace, db.TEST_SET, id)));
        db.delete(FireflyRecord.getKey(db.namespace, db.TEST_SET, id));
        assertNull(db.read(FireflyRecord.getKey(db.namespace, db.TEST_SET, id)));
    }

    @Test
    void testDropDatabase() throws InterruptedException {
        Object id = "foo";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
        db.dropDatabase();

        assertNull(db.read(FireflyRecord.getKey(db.namespace, db.TEST_SET, id)));
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
        db.zeroIdCounter(db.GLOBAL);
        db.incrementAndGetIdCounter(FireflyVertex.class,db.GLOBAL);
        assertEquals(1, db.getIdCounter(db.GLOBAL));
        db.decrementIdCounter(db.GLOBAL);
        assertEquals(0, db.getIdCounter(db.GLOBAL));
        db.incrementAndGetIdCounter(FireflyVertex.class, db.GLOBAL);
        db.incrementAndGetIdCounter(FireflyVertex.class, db.GLOBAL);
        assertEquals(2, db.getIdCounter(db.GLOBAL));
        db.zeroIdCounter(db.GLOBAL);
        assertEquals(0, db.getIdCounter(db.GLOBAL));
    }

    @Test
    void testScanVertexIds() {
        FireflyGraph graph = FireflyGraph.open(configuration);
        ArrayList<Long> ids = new ArrayList<>() {{
            add(0L);
            add(1L);
        }};
        db.writeVertex(graph, ids.get(0), "a");
        db.writeVertex(graph, ids.get(1), "b");
        Iterator<Long> i = db.scanAllIdsInSet(db.VERTEX_AERO_SET);
        assertTrue(i.hasNext());
        Long a = i.next();
        assertTrue(ids.contains(a));
        Long b = i.next();
        assertTrue(ids.contains(b));
    }

    @Test
    void testScanEdgeIds() {
        FireflyGraph graph = FireflyGraph.open(configuration);
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
        Iterator<Long> i = db.scanAllIdsInSet(db.VERTEX_AERO_SET);
        assertTrue(i.hasNext());
        Long a = i.next();
        assertTrue(vertexIds.contains(a));
        Long b = i.next();
        assertTrue(vertexIds.contains(b));

        db.writeEdge(graph, edgeIds.get(0), "anything", va, vb, new Object[]{});
        db.writeEdge(graph, edgeIds.get(1), "anything", vb, va, new Object[]{});

        Iterator<Long> ie = db.scanAllIdsInSet(db.EDGE_AERO_SET);
        assertTrue(ie.hasNext());
        Long ae = ie.next();
        assertTrue(edgeIds.contains(ae));
        Long be = ie.next();
        assertTrue(edgeIds.contains(be));
    }

    @Test
    void testScanQuery() {
        long id1 = 1L;
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id1, bin1, bin2, bin3);
        long id2 = 2L;
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        Bin bin23 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id2, bin21, bin22, bin23);
        Iterator<Map.Entry<Key, Record>> i = db.scanAllRecordsInSet(db.TEST_SET);
        HashMap<Key, Record> results = new HashMap<>();
        i.forEachRemaining(entry -> {
            results.put(entry.getKey(), entry.getValue());
        });


//        assertEquals(results.get(id1).getValue("name"), bin1.value.getObject());
//        assertEquals(results.get(id2).getValue("name"), bin21.value.getObject());
    }

    @Test
    void testFireflyRecordIntegerId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(configuration);
        Integer intId = 1;
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, intId, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, intId);
        assertEquals(record.id(), intId);
    }

    @Test
    void testFireflyRecordLongId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(configuration);
        Long longId = 1L;
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, longId, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, longId);
        assertEquals(record.id(), longId);
    }

}
