package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
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
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
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
import org.junit.Assert;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.stripAllWhiteSpace;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.Sets.TEST_SET;
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
        final Object id = "foo";
        final Bin bin1 = new Bin("name", "John Doe");
        final Bin bin2 = new Bin("age", 32);
        final Bin bin3 = new Bin("greeting", "Hello World!");
        final Operation bin1Op = Operation.put(bin1);
        final Operation bin2Op = Operation.put(bin2);
        final Operation bin3Op = Operation.put(bin3);
        db.writeOperate(null, getKey(db, db.TEST_SET, db.getIdFactory().getTestId(id)), bin1Op, bin2Op, bin3Op);
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, db.getIdFactory().getTestId(id))).record().getInt("age"), 32);
    }

    @Test
    public void testBasicDelete() {
        final FireflyId id = db.getIdFactory().getTestId("1");
        final Bin bin1 = new Bin("name", "John Doe");
        final Bin bin2 = new Bin("age", 32);
        final Bin bin3 = new Bin("greeting", "Hello World!");
        final Operation bin1Op = Operation.put(bin1);
        final Operation bin2Op = Operation.put(bin2);
        final Operation bin3Op = Operation.put(bin3);
        db.writeOperate(null, getKey(db, db.TEST_SET, id), bin1Op, bin2Op, bin3Op);
        final Policy policy = new Policy();
        assertNotEquals(null, db.read(getKey(db, db.TEST_SET, id), policy));
        db.delete(getKey(db, db.TEST_SET, id), null);
        assertNull(db.read(getKey(db, db.TEST_SET, id), policy));
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
        final FireflyId intId = db.getIdFactory().getTestId(1);
        final Bin bin21 = new Bin("name", "Jane Doe");
        final Bin bin22 = new Bin("age", 32);
        final Operation bin21Op = Operation.put(bin21);
        final Operation bin22Op = Operation.put(bin22);
        db.writeOperate(null, getKey(db, db.TEST_SET, intId), bin21Op, bin22Op);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, intId);
        assertEquals(record.id(), intId.getUserId());
    }

    @Test
    public void testFireflyRecordLongId() {
        final FireflyId fid = db.getIdFactory().getTestId(1L);
        final Bin bin21 = new Bin("name", "Jane Doe");
        final Bin bin22 = new Bin("age", 32);
        final Operation bin21Op = Operation.put(bin21);
        final Operation bin22Op = Operation.put(bin22);
        db.writeOperate(null, getKey(db, db.TEST_SET, fid), bin21Op, bin22Op);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, fid);
        assertEquals(record.id(), fid.getUserId());
    }

    @Test
    public void testCreateDropIndex() {
        String binName = "aBin";
        db.createIndex(new ArrayList<>(), db.TEST_SET, "testIndex", binName, IndexType.STRING, IndexCollectionType.LIST);
        db.dropIndex(db.TEST_SET, "testIndex");
    }

    private long countQueryResults(final Statement stmt) {
        final AerospikeConnection.FireflyRecordSet rs = db.query(null, stmt);
        int count = 0;
        while (rs.next()) {
            count++;
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

        long stringCount = countQueryResults(stringQuery);
        assertEquals(50, stringCount);

        final Statement numberQuery = new Statement();
        numberQuery.setNamespace(db.getNamespace());
        numberQuery.setSetName(db.TEST_SET);
        numberQuery.setFilter(Filter.contains(binName, IndexCollectionType.MAPVALUES, 1));
        numberQuery.setIndexName(numberIndex);
        long numberCount = countQueryResults(numberQuery);
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
        AerospikeConnection.FireflyRecordSet rs = db.query(null, stmt);
        Iterator<KeyRecord> i = rs.iterator();
        int count = 0;
        while (i.hasNext()) {
            count++;
            i.next();
        }
        System.out.println(count);
        db.dropIndex(db.TEST_SET, "testIndex");
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
        String infoResponse = AerospikeConnection.InfoOps.singleNodeInfoRequest(db, infoQuery);
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
            final Record data = db.read(key, null);
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

            final Set<Vertex> actualVertices = g.V().toSet();
            // "123", 1234, and 12345L were inserted and should be retrieved as such.
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
        assertTrue(AerospikeConnection.InfoOps.isEnterprise(db));
    }

    @Test
    public void testListEmptySets() {
        Set<String> res = AerospikeConnection.InfoOps.getNonEmptySetList(db);
        System.out.println(res);
    }

    @Test
    public void testClearNamespace() throws InterruptedException {
        Set<String> res = AerospikeConnection.InfoOps.getNonEmptySetList(db);
        System.out.println(res);
        db.clearNamespace();
        final int max = 30;
        int retry = 0;
        while (true) {
            Set<String> res2 = AerospikeConnection.InfoOps.getNonEmptySetList(db);
            try {
                // for GHA we can have "USAGE_STATS_SET", locally can be empty
                assertTrue(res2.size() < 2);
                if (res2.size() == 1) {
                    assertEquals("0_13", res2.iterator().next());
                }
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
        graph.getBaseGraph().dropDatabase(graph, false);
        AerospikeConnection.InfoOps.getNonEmptySetList(db).forEach(nonEmptySet -> {
            db.truncate(null, nonEmptySet, null);
        });

        // Disable drop strategy to test this
        config.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
            while (AerospikeConnection.InfoOps.getNonEmptySetList(db).size() == 0)
                sleep(1000);
            // Add extra long sleep since metadata task sometimes is busy or sleeping and comes in late
            // and isn't removed.
            sleep(5000);
            graph.traversal().V().drop().iterate();
            sleep(10000);

            // ID_MGR_SET  id manager set and G_META graph metadata are not removed by removing all vertices
            Set<String> x = AerospikeConnection.InfoOps.getNonEmptySetList(db);
            // todo: double check about USAGE_STATS_SET
            assertEquals(Set.of(db.GRAPH_METADATA_SET, db.ID_MANAGER_SET, db.SUMMARY_SET), x);

            assertEquals(3, x.size());

            Vertex a = graph.addVertex();
            Vertex b = graph.addVertex();
            Edge e = a.addEdge("edge", b);
            assertEquals(5, AerospikeConnection.InfoOps.getNonEmptySetList(db).size());

            graph.traversal().V().drop().iterate();
            sleep(2000);
            final Iterator<FireflyId> vertexKeys = graph.graphQuery.scanVertexIds(evaluationTimeout);
            final Iterator<FireflyId> edgeKeys = graph.graphQuery.scanEdgeIds(evaluationTimeout);
            assertFalse(vertexKeys.hasNext());
            assertFalse(edgeKeys.hasNext());
        } finally {
            config.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void shouldReadBatchRecords() {
        Key aKey = new Key(db.getNamespace(), db.TEST_SET, "aKey");
        Key bKey = new Key(db.getNamespace(), db.TEST_SET, "bKey");
        Key cKey = new Key(db.getNamespace(), db.TEST_SET, "cKey");
        db.writeOperate(null, aKey, Operation.put(new Bin("bin", 1)));
        db.writeOperate(null, bKey, Operation.put(new Bin("bin", 1)));
        db.writeOperate(null, cKey, Operation.put(new Bin("bin", 1)));
        Record[] data = db.dynamicBatchRead(new Key[]{aKey, bKey, cKey}, null, null);
        assertEquals(3, data.length);
    }

    @Test
    public void updateListByOperation() {
        db.dropDatabase(graph, false);
        final String edgeLabel = "testLabel";
        final String edgeDirection = "OUT";
        final long edgeRawId = 3L;
        final long additionalEdgeRawId = 4L;
        final long vertexRawId = 1L;
        final FireflyId vertexFid = db.getIdFactory().getTestId(vertexRawId);
        final Map<String, List<Long>> labelEdges = new TreeMap<>();
        labelEdges.put(edgeLabel, new ArrayList<>() {{
            add(edgeRawId);
        }});
        final Bin edgeDataBin = new Bin(edgeDirection, Value.get(labelEdges, MapOrder.KEY_ORDERED));
        final Operation edgeDataBinOp = Operation.put(edgeDataBin);
        db.writeOperate(null, getKey(db, db.TEST_SET, vertexFid), edgeDataBinOp);
        final Key vertexAeroKey = new Key(db.getNamespace(), TEST_SET.name(), (Long) vertexFid.getUserId());
        Record operateResultRecord = db.writeOperate(null, vertexAeroKey,
                ListOperation.append(edgeDirection, Value.get(additionalEdgeRawId), CTX.mapKey(Value.get(edgeLabel))),
                Operation.get(edgeDirection)
        );
        Record record = db.read(vertexAeroKey, null);
        Map<String, List<Long>> labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(2, labelEdgesRetrieved.get(edgeLabel).size());

        Record operateResultRecord2 = db.writeOperate(null, vertexAeroKey,
                ListOperation.removeByValue(edgeDirection, Value.get(edgeRawId), ListReturnType.NONE, CTX.mapKey(Value.get(edgeLabel))),
                Operation.get(edgeDirection)
        );

        record = db.read(vertexAeroKey, null);
        labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(1, labelEdgesRetrieved.get(edgeLabel).size());
        assertEquals(additionalEdgeRawId, labelEdgesRetrieved.get(edgeLabel).get(0).longValue());
    }

    @Test
    public void createListByOperation() {
        db.dropDatabase(graph, false);
        final String edgeLabel = "testLabel";
        final String edgeDirection = "OUT";
        final long edgeRawId = 3L;
        final long additionalEdgeRawId = 4L;
        final long vertexRawId = 1L;
        final FireflyId vertexFid = db.getIdFactory().getTestId(vertexRawId);
        final Map<String, List<Long>> labelEdges = new TreeMap<>();

        final Bin edgeDataBin = new Bin(edgeDirection, Value.get(labelEdges));
        final Operation edgeDataBinOp = Operation.put(edgeDataBin);
        db.writeOperate(null, getKey(db, db.TEST_SET, vertexFid), edgeDataBinOp);
        final Key vertexAeroKey = new Key(db.getNamespace(), TEST_SET.name(), (Long) vertexFid.getUserId());
        Record operateResultRecord = db.writeOperate(null, vertexAeroKey,
                ListOperation.append(edgeDirection, Value.get(additionalEdgeRawId), CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)),
                Operation.get(edgeDirection)
        );
        Record record = db.read(vertexAeroKey, null);
        Map<String, List<Long>> labelEdgesRetrieved = (Map<String, List<Long>>) record.getMap(edgeDirection);
        assertEquals(1, labelEdgesRetrieved.get(edgeLabel).size());

        Record operateResultRecord2 = db.writeOperate(null, vertexAeroKey,
                ListOperation.removeByValue(edgeDirection, Value.get(additionalEdgeRawId), ListReturnType.NONE, CTX.mapKey(Value.get(edgeLabel))),
                Operation.get(edgeDirection)
        );

        record = db.read(vertexAeroKey, null);
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
    public void testKeyRead() {
        FireflyVertex va = (FireflyVertex) graph.addVertex(T.id, "A");
        FireflyVertex vb = (FireflyVertex) graph.addVertex(T.id, "B");

        Key keyaObj = new Key(db.getNamespace(), db.VERTEX_AERO_SET, Value.get(va.id()));
        Key keybObj = new Key(db.getNamespace(), db.VERTEX_AERO_SET, Value.get(vb.id()));
        final Record[] records = db.dynamicBatchRead(new Key[]{keyaObj, keybObj}, null, null);
        assertEquals(2, records.length);

        Key keyaHash = new Key(db.getNamespace(), va.id.getKeyHash(), db.VERTEX_AERO_SET, Value.NULL);
        Key keybHash = new Key(db.getNamespace(), vb.id.getKeyHash(), db.VERTEX_AERO_SET, Value.NULL);
        final Record[] hashRecords = db.dynamicBatchRead(new Key[]{keyaHash, keybHash}, null, null);
        assertEquals(2, hashRecords.length);
    }

    @Test
    public void testHostsWhiteSpaceStripping() {
        final String singleHostname = " localhost  ";
        assertEquals("localhost", stripAllWhiteSpace(singleHostname));
        final String singleIP = "  172.17.0.1 ";
        assertEquals("172.17.0.1", stripAllWhiteSpace(singleIP));
        final String singleHostnamePort = " localhost:3000  ";
        assertEquals("localhost:3000", stripAllWhiteSpace(singleHostnamePort));
        final String singleIPPort = "  172.17.0.1: 3000 ";
        assertEquals("172.17.0.1:3000", stripAllWhiteSpace(singleIPPort));
        final String multi = " l ocalhost , aerospike.com, github.co m";
        assertEquals("localhost,aerospike.com,github.com", stripAllWhiteSpace(multi));
        final String multiPort = " localhost: 30 00,aerospike.com: 8080,github.com:8192";
        assertEquals("localhost:3000,aerospike.com:8080,github.com:8192", stripAllWhiteSpace(multiPort));
    }

    @Test
    public void testDynamicEdgeCacheSizing() {
        // TODO https://aerospike.atlassian.net/browse/GRAPH-982: When dynamic aerospike.conf is fixed for our CI
        //  workflow, we can make this test better. For now just test it matches max-record-size=0 and
        //  write-block-size=128k
        final Configuration configuration = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final AerospikeConnection db = AerospikeConnection.connect(configuration)) {
            Assert.assertEquals(6553, db.ON_RECORD_ID_LIMIT);
        }
        configuration.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), "2000");
        try (final AerospikeConnection db = AerospikeConnection.connect(configuration)) {
            Assert.assertEquals(2000, db.ON_RECORD_ID_LIMIT);
        }
    }
}
