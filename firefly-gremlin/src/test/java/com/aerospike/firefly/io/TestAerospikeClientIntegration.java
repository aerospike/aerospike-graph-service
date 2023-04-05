package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIterator;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.PerfUtil;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.Sets.TEST_SET;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestAerospikeClientIntegration extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testConnectToAerospike() {
        Configuration configuration = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection test_db = AerospikeConnection.connect(configuration);
        test_db.close();
    }

    @Test
    public void testBasicReadWrite() {
        Object id = "foo";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.writeElement(db, db.TEST_SET, FireflyIdPoly.fromObject((String) id, db.TEST_SET), -1, bin1, bin2, bin3);
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, FireflyIdPoly.fromObject((String) id, db.TEST_SET))).record().getInt("age"), 32);
    }

    @Test
    public void testBasicDelete() {
        FireflyId id = FireflyIdPoly.fromObject("1", db.TEST_SET);
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.writeElement(db, db.TEST_SET, id, -1, bin1, bin2, bin3);
        final Policy policy = new Policy();
        policy.sendKey = false;
        assertNotEquals(null, db.read(FireflyRecord.getKey(db, db.TEST_SET, id), policy));
        db.delete(FireflyRecord.getKey(db, db.TEST_SET, id));
        assertNull(db.read(FireflyRecord.getKey(db, db.TEST_SET, id), policy));
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
    public void testSyntheticSupernode() {
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT.toLowerCase(), "5");

        try (FireflyGraph graph = FireflyGraph.open(config)) {
            config.clearProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT.toLowerCase());
            graph.traversal().V().drop().iterate();
            Vertex root = graph.addVertex("root");
            IntStream.range(0, 6).forEach(i -> {
                Vertex nu = graph.addVertex("leaf");
                graph.traversal().V(root).addE("edge").to(nu).next();
            });
            FireflyVertex x = (FireflyVertex) graph.traversal().V(root).next();
            Record br = x.getBaseElement();
            long val = graph.traversal().V(root).bothE().count().next().longValue();
            assertEquals(6L, val);
            IntStream.range(0, 2).forEach(i -> graph.traversal().E().limit(1).drop().iterate());

            assertEquals(4L, graph.traversal().V(root).bothE().count().next().longValue());
        }
    }

    @Test
    public void testSyntheticSupernodeCompositeId() {
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT.toLowerCase(), "5");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            config.clearProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT.toLowerCase());
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final Vertex root = g.addV("root").next();
            IntStream.range(0, 100).forEach(i -> {
                Vertex nu = graph.traversal().addV("leaf").property("name", "leaf" + i).next();
                graph.traversal().V(root).addE("edge").to(nu).next();
            });
            final List<Vertex> leaf1 = g.V(root.id()).out().has("name", "leaf1").toList();
            assertEquals(1, leaf1.size());
        }
    }

    @Test
    public void testFireflyRecordIntegerId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(config);
        FireflyId intId = FireflyIdPoly.fromObject(1, db.TEST_SET);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.writeElement(db, db.TEST_SET, intId, -1, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, intId);
        assertEquals(record.id(), intId.getUserId());
    }

    @Test
    public void testFireflyRecordLongId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(config);
        FireflyId fid = FireflyIdPoly.fromObject(1L, db.TEST_SET);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.writeElement(db, db.TEST_SET, fid, -1, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, fid);
        assertEquals(record.id(), fid.getUserId());
    }

    @Test
    public void testCreateDropIndex() {
        String binName = "aBin";
        db.createIndex(new ArrayList<>(), db.TEST_SET, "testIndex", binName, IndexType.STRING, IndexCollectionType.LIST);
        db.dropIndex(db.TEST_SET, "testIndex");
    }

    private long countQueryResults(final String binName, final String testIndex, final Statement stmt) {
        QueryPolicy p = new QueryPolicy();
        RecordSet rs = db.getClient().query(p, stmt);
        int count = 0;
        try {
            while (rs.next())
                count++;
        } catch (AerospikeException e) {
            throw e;
        }
        return count;
    }

    @Test
    public void testWriteReadMapUsingIndex() throws InterruptedException {
        final String mapKey = "choice";
        final Map<String, Object> aMap = new HashMap<>() {{
            put(mapKey, "a");
        }};
        final Map<String, Object> bMap = new HashMap<>() {{
            put(mapKey, "a");
        }};
        final Map<String, Object> cMap = new HashMap<>() {{
            put(mapKey, "c");
        }};
        final Map<String, Object> oneMap = new HashMap<>() {{
            put(mapKey, 1);
        }};

        final String binName = "choiceMap";
        final String stringIndex = "stringIndex";
        final String numberIndex = "numberIndex";
        final Iterator<Map<String, Object>> choices = Iterables.cycle(aMap, bMap, cMap, oneMap).iterator();
        db.createIndex(new ArrayList<>(), db.TEST_SET, stringIndex, binName, IndexType.STRING, IndexCollectionType.MAPVALUES);
        db.createIndex(new ArrayList<>(), db.TEST_SET, numberIndex, binName, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);

        IntStream.range(0, 100).forEach(i -> {
            final Key key = new Key(db.getNamespace(), db.TEST_SET, i);
            db.checkedPut(null, key, new Bin(binName, choices.next()));
        });
        final Statement stringQuery = new Statement();
        stringQuery.setNamespace(db.getNamespace());
        stringQuery.setSetName(db.TEST_SET);
        stringQuery.setFilter(Filter.contains(binName, IndexCollectionType.MAPVALUES, "a"));
        stringQuery.setIndexName(stringIndex);

        long stringCount = countQueryResults(binName, stringIndex, stringQuery);
        assertEquals(50, stringCount);

        final Statement numberQuery = new Statement();
        numberQuery.setNamespace(db.getNamespace());
        numberQuery.setSetName(db.TEST_SET);
        numberQuery.setFilter(Filter.contains(binName, IndexCollectionType.MAPVALUES, 1));
        numberQuery.setIndexName(numberIndex);
        long numberCount = countQueryResults(binName, stringIndex, numberQuery);
        assertEquals(25, numberCount);

        db.dropIndex(db.TEST_SET, numberIndex);
        db.dropIndex(db.TEST_SET, stringIndex);

    }

    @Test
    public void testWriteReadUsingIndex() {
        final String binName = "age";
        final String testIndex = "testIndex";
        db.createIndex(new ArrayList<>(), db.TEST_SET, testIndex, binName, IndexType.NUMERIC, IndexCollectionType.DEFAULT);
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin3 = new Bin("greeting", "Hello World!");
        IntStream.range(0, 100).forEach(i -> {
            final Key key = new Key(db.getNamespace(), db.TEST_SET, i);
            db.checkedPut(null, key, bin1, new Bin("weight", 2000 + i), new Bin("age", 32 + i), bin3);
        });

        Statement stmt = new Statement();
        stmt.setNamespace(db.getNamespace());
        stmt.setSetName(db.TEST_SET);
        stmt.setFilter(Filter.range("age", 34, 99));
        QueryPolicy p = new QueryPolicy();
        RecordSet rs = db.getClient().query(null, stmt);
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
    public void testParseRaw() {
        final String infoResponse = Info.request(new InfoPolicy(), db.getClient().getNodes()[0], "namespaces");
        List<Map<String, String>> data = AerospikeConnection.InfoOps.parseRaw(infoResponse);
        final AtomicBoolean pass = new AtomicBoolean(false);
        data.forEach(it -> {
            if (it.containsKey(AerospikeConnection.InfoOps.Keys.RESULT) && Objects.equals(it.get(AerospikeConnection.InfoOps.Keys.RESULT), "test"))
                pass.set(true);
        });
        assertTrue(pass.get());
    }

    @Ignore
    @Test
    public void testAerospikeInfo() {
        final String binName = "age";
        final String testIndex = "testIndex";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin3 = new Bin("greeting", "Hello World!");
        int NUMBER_OF_RECORDS = 100;
        IntStream.range(0, NUMBER_OF_RECORDS).forEach(i -> {
            final Key key = new Key(db.getNamespace(), db.TEST_SET, i);
            db.checkedPut(null, key, bin1, new Bin("weight", 2000 + i), new Bin("age", 32 + i), bin3);
        });

        String infoQuery = "sets/" + db.getNamespace() + "/" + db.TEST_SET;
        String infoResponse = Info.request(new InfoPolicy(), db.getClient().getNodes()[0], infoQuery);
        Long reportedObjectCount = Arrays.stream(infoResponse.split(":"))
                .filter(str -> str.startsWith("objects"))
                .map(str -> Long.valueOf(str.split("=")[1]))
                .collect(Collectors.toList())
                .get(0);

        assertEquals(reportedObjectCount, Long.valueOf(NUMBER_OF_RECORDS));
    }

    @Test
    public void testAerospikeReadLatency() {
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        IntStream.range(0, 100).forEach(i -> {
            final Key key = new Key(db.getNamespace(), db.TEST_SET, i);
            db.checkedPut(null, key, bin1, bin2, bin3);
        });

        PerfUtil.Results results = PerfUtil.runTestBatch(100, () -> {
            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            final Key key = new Key(db.getNamespace(), db.TEST_SET, tlr.nextInt(0, 100));
            final Record data = db.getClient().get(null, key);
            assert data.getLong("age") == 32;
        });
        LOG.info(results.toString());
    }

    @Test
    public void testFireflyVertexWithUserSuppliedId() {
        // Ids that are strings will be parsed to longs. Integer Ids will be inserted as integers and longs as longs.
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            g.addV("user-id-vertex").property(T.id, "123").
                    addV("user-id-vertex").property(T.id, 1234).
                    addV("user-id-vertex").property(T.id, 12345L).
                    iterate();
            assertEquals(3L, g.V().count().next().longValue());

            // "123", 1234, and 12345L were inserted and should be retrieved as such.
            final Set<Vertex> actualVertices = g.V().toSet();
            final Set<Object> expectedIds = ImmutableSet.of("123", 1234, 12345L);
            final Set<Object> actualIds = actualVertices.stream().map(Vertex::id).collect(Collectors.toSet());
            assertEquals(expectedIds, actualIds);


            // If a value already exists in the graph then adding it again should throw an IllegalArgumentException.
            // Try "123", 123, and 123L, all should fail.
            assertThrows(IllegalArgumentException.class, () ->
                    g.addV("user-id").property(T.id, "123").iterate());
            assertThrows(IllegalArgumentException.class, () ->
                    g.addV("user-id").property(T.id, 123).iterate());
            assertThrows(IllegalArgumentException.class, () ->
                    g.addV("user-id").property(T.id, 123L).iterate());
        }
    }

    @Test
    public void testIsEnterprise() {
        assertTrue(AerospikeConnection.InfoOps.isEnterprise(db.getClient()));
    }

    @Test
    public void testListAllSets() {
        Set<String> res = AerospikeConnection.InfoOps.getSetList(db.getNamespace(), db.getClient());
    }

    @Test
    public void testListEmptySets() {
        Set<String> res = AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient());
        System.out.println(res);
    }

    @Test
    public void testClearNamespace() throws InterruptedException {
        Set<String> res = AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient());
        System.out.println(res);
        db.clearNamespace();
        final int max = 30;
        int retry = 0;
        while (true) {
            Set<String> res2 = AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient());
            try {
                assertTrue(res2.isEmpty());
            } catch (AssertionError e) {
                System.out.println(res2);
                retry = retry + 1;
                if (retry > max) {
                    throw e;
                }
                sleep(1000);
                continue;
            }
            break;
        }
    }

    @Test
    public void shouldRemoveAllData() throws InterruptedException {
        graph.getBaseGraph().dropDatabase();
        AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).forEach(nonEmptySet -> {
            db.getClient().truncate(null, db.getNamespace(), nonEmptySet, null);
        });

        // Disable drop strategy to test this
        config.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
            while (AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size() == 0)
                sleep(1000);
            graph.traversal().V().drop().iterate();
            sleep(10000);

            // ID_MGR_SET id manager set and G_META graph metadata are not removed by removing all vertices
            Set<String> x = AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient());
            assertEquals(!StarPackedGraph.isStarPackedGraph(graph) ?
                    Set.of("0_G_META", "0_ID_MGR_SET") :
                    Set.of("0_IN_IN", "0_G_META", "0_OUT_OUT", "0_OUT_IN", "0_OUT_VP", "0_IN_OUT", "0_IN_VP"), x);
            assertEquals(!StarPackedGraph.isStarPackedGraph(graph) ? 2 : 7, x.size());

            Vertex a = graph.addVertex();
            Vertex b = graph.addVertex();
            Edge e = a.addEdge("edge", b);
            assertEquals(!StarPackedGraph.isStarPackedGraph(graph) ? 4 : 10, AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size());

            graph.traversal().V().drop().iterate();
            sleep(2000);
            Iterator<KeyRecord> vertxKeys = db.scanAllKeysInSet(db.VERTEX_AERO_SET, null);
            Iterator<KeyRecord> edgeKeys = db.scanAllRecordsInSet(db.EDGE_AERO_SET, null, new ScanPolicy(),
                    AerospikeConnection.LABEL);
            FireflyPhatEdgeIdIterator edges = new FireflyPhatEdgeIdIterator(edgeKeys, db);
            assertFalse(vertxKeys.hasNext());
            assertFalse(edges.hasNext());
        } finally {
            config.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void shouldReadBatchRecords() {
        Key aKey = new Key(db.getNamespace(), db.TEST_SET, "aKey");
        Key bKey = new Key(db.getNamespace(), db.TEST_SET, "bKey");
        Key cKey = new Key(db.getNamespace(), db.TEST_SET, "cKey");
        db.write(aKey, new Bin("bin", 1));
        db.write(bKey, new Bin("bin", 1));
        db.write(cKey, new Bin("bin", 1));
        Record[] data = db.read(new Key[]{aKey, bKey, cKey});
        assertEquals(3, data.length);
    }

    @Test
    public void updateListByOperation() {
        db.dropDatabase();
        final String edgeLabel = "testLabel";
        final String edgeDirection = "OUT";
        final long edgeRawId = 3L;
        final long additionalEdgeRawId = 4L;
        final long vertexRawId = 1L;
        final FireflyId vertexFid = FireflyIdPoly.fromObject(vertexRawId, db.TEST_SET);
        final Map<String, List<Long>> labelEdges = new TreeMap<>();
        labelEdges.put(edgeLabel, new ArrayList<>() {{
            add(edgeRawId);
        }});
        final Bin edgeDataBin = new Bin(edgeDirection, Value.get(labelEdges, MapOrder.KEY_ORDERED));
        final Bin[] bins = new Bin[]{edgeDataBin};
        FireflyRecord.writeElement(db, TEST_SET, vertexFid, -1, bins);
        final Key vertexAeroKey = new Key(db.getNamespace(), TEST_SET, (Long) vertexFid.getUserId());
        Record operateResultRecord = db.operate(null, vertexAeroKey,
                ListOperation.append(edgeDirection, Value.get(additionalEdgeRawId), CTX.mapKey(Value.get(edgeLabel))),
                Operation.get(edgeDirection)
        );
        Record record = db.getClient().get(null, vertexAeroKey);
        Map<String, List<Long>> labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(2, labelEdgesRetrieved.get(edgeLabel).size());

        Record operateResultRecord2 = db.operate(null, vertexAeroKey,
                ListOperation.removeByValue(edgeDirection, Value.get(edgeRawId), ListReturnType.NONE, CTX.mapKey(Value.get(edgeLabel))),
                Operation.get(edgeDirection)
        );

        record = db.getClient().get(null, vertexAeroKey);
        labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(1, labelEdgesRetrieved.get(edgeLabel).size());
        assertEquals(additionalEdgeRawId, labelEdgesRetrieved.get(edgeLabel).get(0).longValue());
    }

    @Test
    public void createListByOperation() {
        db.dropDatabase();
        final String edgeLabel = "testLabel";
        final String edgeDirection = "OUT";
        final long edgeRawId = 3L;
        final long additionalEdgeRawId = 4L;
        final long vertexRawId = 1L;
        final FireflyId vertexFid = FireflyIdPoly.fromObject(vertexRawId, db.TEST_SET);
        final Map<String, List<Long>> labelEdges = new TreeMap<>();

        final Bin edgeDataBin = new Bin(edgeDirection, Value.get(labelEdges));
        final Bin[] bins = new Bin[]{edgeDataBin};
        FireflyRecord.writeElement(db, TEST_SET, vertexFid, -1, bins);
        final Key vertexAeroKey = new Key(db.getNamespace(), TEST_SET, (Long) vertexFid.getUserId());
        Record operateResultRecord = db.operate(null, vertexAeroKey,
                ListOperation.append(edgeDirection, Value.get(additionalEdgeRawId), CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)),
                Operation.get(edgeDirection)
        );
        Record record = db.getClient().get(null, vertexAeroKey);
        Map<String, List<Long>> labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(1, labelEdgesRetrieved.get(edgeLabel).size());

        Record operateResultRecord2 = db.operate(null, vertexAeroKey,
                ListOperation.removeByValue(edgeDirection, Value.get(additionalEdgeRawId), ListReturnType.NONE, CTX.mapKey(Value.get(edgeLabel))),
                Operation.get(edgeDirection)
        );

        record = db.getClient().get(null, vertexAeroKey);
        labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(0, labelEdgesRetrieved.get(edgeLabel).size());
    }

    @Test
    public void testKeyHash() {
        //Aerospike Java Client BC
        //Aerospike Java client interface to Aerospike database server. Uses Bouncy Castle crypto library for RIPEMD-160 hashing.
        final String SET_NAME = "testSet";
        final String KEY_NAME = "testKey";
        final Value keyValue = Value.get(KEY_NAME);
        final byte[] digest = Crypto.computeDigest(SET_NAME, keyValue);
        final String computedHash = Crypto.encodeBase64(digest);


        final Key key = new Key("test", SET_NAME, KEY_NAME);
        assertEquals(Crypto.encodeBase64(key.digest), computedHash);
    }

    @Test
    public void testRecoverOriginalUserKeyFromHash() {
        final String SET_NAME = "testSet";
        final String KEY_NAME = "testKey";
        final Value keyValue = Value.get(KEY_NAME);
        final byte[] digest = Crypto.computeDigest(SET_NAME, keyValue);
        final String computedHashString = Crypto.encodeBase64(digest);
        final Key key = new Key("test", digest, SET_NAME, Value.NULL);
        db.write(key, new Bin("bin", 1));

        final Policy policy = new Policy();
        policy.sendKey = false;
        Record result = db.read(key, policy);
        class TestRL implements RecordListener {
            public Key key;
            Semaphore semaphore = new Semaphore(0);
            private Record record;

            @Override
            public void onSuccess(Key key, Record record) {
                this.key = key;
                this.record = record;
                semaphore.release();
            }

            @Override
            public void onFailure(AerospikeException e) {
                semaphore.release();
                throw new RuntimeException(e);
            }

            public KeyRecord get() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return new KeyRecord(key, record);
            }
        }
        TestRL testRL = new TestRL();
        final BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.sendKey = false;
        db.getClient().get(db.getEventLoops().get(0), testRL, batchPolicy, key);
        KeyRecord keyRecord = testRL.get();
        assertEquals(1, keyRecord.record.getInt("bin"));
        //Cant recover the original key. Seems strange since Scan will send the original key
        Object orig = keyRecord.key.userKey.getObject();
        assertNull(orig); //This should be te original user key, but it is not, its null
    }

    @Test
    public void testKeyRead() {
        PackedVertex va = (PackedVertex) graph.addVertex(T.id, "A");
        PackedVertex vb = (PackedVertex) graph.addVertex(T.id, "B");

        Key keyaObj = new Key(db.getNamespace(), db.VERTEX_AERO_SET, Value.get(va.id()));
        Key keybObj = new Key(db.getNamespace(), db.VERTEX_AERO_SET, Value.get(vb.id()));
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.sendKey = false;
        final Record[] records = db.getClient().get(batchPolicy, new Key[]{keyaObj, keybObj});
        assertEquals(2, records.length);

        Key keyaHash = new Key(db.getNamespace(), va.id.getKeyHash(), db.VERTEX_AERO_SET, Value.NULL);
        Key keybHash = new Key(db.getNamespace(), vb.id.getKeyHash(), db.VERTEX_AERO_SET, Value.NULL);
        final Record[] hashRecords = db.getClient().get(batchPolicy, new Key[]{keyaHash, keybHash});
        assertEquals(2, hashRecords.length);
    }

}
