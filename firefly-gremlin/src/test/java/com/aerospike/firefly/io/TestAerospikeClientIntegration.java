package com.aerospike.firefly.io;

import com.aerospike.client.*;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.*;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.PerfUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.TEST_SET;
import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeClientIntegration {

    private Configuration configuration;
    private AerospikeConnection db;

    @Before
    public void setup() {
        configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        db = AerospikeConnection.connect(configuration);
        db.dropDatabase();
    }

    @After
    public void cleanup() {
        db.dropDatabase();
        db.close();
    }

    @Test
    public void testConnectToAerospike() {
        Configuration configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection test_db = AerospikeConnection.connect(configuration);
        test_db.close();
    }

    @Test
    public void testBasicReadWrite() {
        Object id = "foo";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, FireflyId.of(db, null, id), bin1, bin2, bin3);
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, FireflyId.of(db, null, id))).record.getInt("age"), 32);
    }

    @Test
    public void testBasicReadWriteGetValueOfKey() {
        FireflyId id = FireflyId.of(db, null, "foo");
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
    public void testBasicDelete() {
        FireflyId id = FireflyId.of(db, null, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
        assertNotEquals(null, db.read(FireflyRecord.getKey(db.namespace, db.TEST_SET, id)));
        db.delete(FireflyRecord.getKey(db.namespace, db.TEST_SET, id));
        assertNull(db.read(FireflyRecord.getKey(db.namespace, db.TEST_SET, id)));
    }

    @Test
    public void testDropDatabase() throws InterruptedException {
        FireflyId id = FireflyId.of(db, null, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
        db.dropDatabase();

        assertNull(db.read(FireflyRecord.getKey(db.namespace, db.TEST_SET, id)));
    }


    @Test
    public void testCounterOps() {
        db.zeroIdCounter(db.GLOBAL);
        db.incrementAndGetIdCounter(db.GLOBAL);
        assertEquals(1, db.getIdCounter(db.GLOBAL));
        db.decrementIdCounter(db.GLOBAL);
        assertEquals(0, db.getIdCounter(db.GLOBAL));
        db.incrementAndGetIdCounter(db.GLOBAL);
        db.incrementAndGetIdCounter(db.GLOBAL);
        assertEquals(2, db.getIdCounter(db.GLOBAL));

        long res = db.greaterOrIncrement(36, db.GLOBAL);
        assertEquals(db.getIdCounter(db.GLOBAL), res);
        assertEquals(36, res);

        db.zeroIdCounter(db.GLOBAL);
        assertEquals(0, db.getIdCounter(db.GLOBAL));
        db.incrementAndGetIdCounter(db.GLOBAL);
        db.incrementAndGetIdCounter(db.GLOBAL);
        db.incrementAndGetIdCounter(db.GLOBAL);
        db.incrementAndGetIdCounter(db.GLOBAL);
        assertEquals(4, db.getIdCounter(db.GLOBAL));
        long res2 = db.greaterOrIncrement(3, db.GLOBAL);
        assertEquals(5, db.getIdCounter(db.GLOBAL));
        assertEquals(5, res2);
        assertEquals(5, db.getIdCounter(db.GLOBAL));
        assertEquals(5, db.greaterOrExisting(3, db.GLOBAL));
        assertEquals(5, db.greaterOrExisting(3, db.GLOBAL));
    }

    @Test
    public void testScanVertexIds() {
        FireflyGraph graph = FireflyGraph.open(configuration);
        ArrayList<Long> ids = new ArrayList<>() {{
            add(0L);
            add(1L);
        }};
        db.writeVertex(graph, FireflyId.of(db, FireflyVertex.class, ids.get(0)), "a");
        db.writeVertex(graph, FireflyId.of(db, FireflyVertex.class, ids.get(1)), "b");
        Iterator<Long> i = db.scanAllIdsInSet(db.VERTEX_AERO_SET);
        assertTrue(i.hasNext());
        Long a = i.next();
        assertTrue(ids.contains(a));
        Long b = i.next();
        assertTrue(ids.contains(b));
    }

    @Test
    public void testSyntheticSupernode() {
        configuration.setProperty(ConfigurationHelper.Keys.ID_CACHE_SIZE, "5");
        FireflyGraph graph = FireflyGraph.open(configuration);
        Vertex root = graph.addVertex("root");
        List<Vertex> stuff = new ArrayList<>();
        IntStream.range(0, 6).forEach(i -> {
            Vertex nu = graph.addVertex("leaf");
            stuff.add(nu);
            graph.traversal().V(root).addE("edge").to(nu).next();
        });

        assertEquals(6L, graph.traversal().V(root).bothE().count().next().longValue());
        final Iterator<Vertex> iter = stuff.iterator();
        IntStream.range(0, 2).forEach(i -> {
            graph.traversal().E(iter.next()).drop().tryNext();
        });
        List<Edge> list2 = graph.traversal().V(root).bothE().toList();


        assertEquals(4L, graph.traversal().V(root).bothE().count().next().longValue());
    }


    @Test
    public void testScanEdgeIds() {
        FireflyGraph graph = FireflyGraph.open(configuration);
        ArrayList<Long> vertexIds = new ArrayList<>() {{
            add(0L);
            add(1L);
        }};
        ArrayList<Long> edgeIds = new ArrayList<>() {{
            add(2L);
            add(3L);
        }};
        db.writeVertex(graph, FireflyId.of(db, FireflyVertex.class, vertexIds.get(0)), "a");
        FireflyVertex va = db.readVertex(graph, FireflyId.of(db, FireflyVertex.class, vertexIds.get(0)));
        db.writeVertex(graph, FireflyId.of(db, FireflyVertex.class, vertexIds.get(1)), "b");
        FireflyVertex vb = db.readVertex(graph, FireflyId.of(db, FireflyVertex.class, vertexIds.get(1)));
        Iterator<Long> i = db.scanAllIdsInSet(db.VERTEX_AERO_SET);
        assertTrue(i.hasNext());
        Long a = i.next();
        assertTrue(vertexIds.contains(a));
        Long b = i.next();
        assertTrue(vertexIds.contains(b));

        db.writeEdge(graph, FireflyId.of(db, FireflyEdge.class, edgeIds.get(0)), "anything", va, vb, new Object[]{});
        db.writeEdge(graph, FireflyId.of(db, FireflyEdge.class, edgeIds.get(1)), "anything", vb, va, new Object[]{});

        Iterator<Long> ie = db.scanAllIdsInSet(db.EDGE_AERO_SET);
        assertTrue(ie.hasNext());
        Long ae = ie.next();
        assertTrue(edgeIds.contains(ae));
        Long be = ie.next();
        assertTrue(edgeIds.contains(be));
    }

    @Test
    public void testScanQuery() {
        FireflyId id1 = FireflyId.of(db, null, 1L);
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id1, bin1, bin2, bin3);
        FireflyId id2 = FireflyId.of(db, null, 2L);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        Bin bin23 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id2, bin21, bin22, bin23);
        Iterator<Map.Entry<Key, Record>> i = db.scanAllRecordsInSet(db.TEST_SET);
        HashMap<Key, Record> results = new HashMap<>();
        i.forEachRemaining(entry -> {
            results.put(entry.getKey(), entry.getValue());
        });
    }

    @Test
    public void testFireflyRecordIntegerId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(configuration);
        FireflyId intId = FireflyId.of(db, null, 1);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, intId, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, intId);
        assertEquals(record.id(), intId.value());
    }

    @Test
    public void testFireflyRecordLongId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(configuration);
        FireflyId fid = FireflyId.of(db, null, 1L);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, fid, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, fid);
        assertEquals(record.id(), fid.value());
    }

    @Test
    public void testCreateDropIndex() {
        String binName = "aBin";
        db.createIndex(db.TEST_SET, "testIndex", binName, IndexType.STRING, IndexCollectionType.LIST);
        db.dropIndex(db.TEST_SET, "testIndex");
    }

    @Test
    public void testWriteReadUsingIndex() {
        final String binName = "age";
        final String testIndex = "testIndex";
        db.createIndex(db.TEST_SET, testIndex, binName, IndexType.NUMERIC, IndexCollectionType.DEFAULT);
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin3 = new Bin("greeting", "Hello World!");
        IntStream.range(0, 10000).forEach(i -> {
            final Key key = new Key(db.namespace, TEST_SET, i);
            db.client.put(null, key, bin1, new Bin("weight", 2000 + i), new Bin("age", 32 + i), bin3);
        });

        Statement stmt = new Statement();
        stmt.setNamespace(db.namespace);
        stmt.setSetName(TEST_SET);
        stmt.setFilter(Filter.range("age", 34, 99));
        QueryPolicy p = new QueryPolicy();
        RecordSet rs = db.client.query(null, stmt);
        Iterator<KeyRecord> i = rs.iterator();
        int count = 0;
        while (i.hasNext()) {
            count++;
            i.next();
        }
        System.out.println(count);
        db.dropIndex(db.TEST_SET, "testIndex");
    }

    @Test
    public void testAerospikeInfo() {
        final String binName = "age";
        final String testIndex = "testIndex";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin3 = new Bin("greeting", "Hello World!");
        int NUMBER_OF_RECORDS = 10000;
        IntStream.range(0, NUMBER_OF_RECORDS).forEach(i -> {
            final Key key = new Key(db.namespace, TEST_SET, i);
            db.client.put(null, key, bin1, new Bin("weight", 2000 + i), new Bin("age", 32 + i), bin3);
        });
        String infoQuery = "sets/" + db.namespace + "/" + TEST_SET;
        String infoResponse = Info.request(new InfoPolicy(), db.client.getNodes()[0], infoQuery);
        Long reportedObjectCount = Arrays.stream(infoResponse.split(":"))
                .filter(str -> str.startsWith("objects"))
                .map(str -> Long.valueOf(str.split("=")[1]))
                .collect(Collectors.toList())
                .get(0);

        assertEquals(reportedObjectCount, Long.valueOf(NUMBER_OF_RECORDS));
    }

    @Test
    public void testCountElements(){
        FireflyGraph graph = FireflyGraph.open(configuration);
        ArrayList<Vertex> added = new ArrayList<>();
        IntStream.range(0,1000).forEach(i -> {
            Vertex nv = graph.addVertex();
            added.add(nv);
            if(i != 0)
                nv.addEdge("test",added.get(0));
        });
        assertEquals((long)graph.traversal().E().count().next(),db.getEdgeCount());
        assertEquals((long)graph.traversal().V().count().next(),db.getVertexCount());
    }
    @Test
    public void testAerospikeReadLatency() {
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        IntStream.range(0, 10000).forEach(i -> {
            final Key key = new Key(db.namespace, TEST_SET, i);
            db.client.put(null, key, bin1, bin2, bin3);
        });

        PerfUtil.Results results = PerfUtil.runTestBatch(10000, () -> {
            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            final Key key = new Key(db.namespace, TEST_SET, tlr.nextInt(0, 10000));
            final Record data = db.client.get(null, key);
            assert data.getLong("age") == 32;
        });
        System.out.println(results);
    }


}
