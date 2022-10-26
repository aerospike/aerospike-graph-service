package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
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
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_PERIODIC_METADATA_UPDATE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.METADATA_UPDATE_FREQUENCY;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

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
        FireflyRecord.write(db, db.TEST_SET, FireflyId.of(null, id), -1, bin1, bin2, bin3);
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, FireflyId.of(null, id))).record.getInt("age"), 32);
    }

    @Test
    public void testBasicDelete() {
        FireflyId id = FireflyId.of(null, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, -1, bin1, bin2, bin3);
        assertNotEquals(null, db.read(FireflyRecord.getKey(db.getNamespace(), db.TEST_SET, id)));
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.TEST_SET, id));
        assertNull(db.read(FireflyRecord.getKey(db.getNamespace(), db.TEST_SET, id)));
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
        config.setProperty(ConfigurationHelper.Keys.ID_CACHE_SIZE, "5");

        try (FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            Vertex root = graph.addVertex("root");
            IntStream.range(0, 6).forEach(i -> {
                Vertex nu = graph.addVertex("leaf");
                graph.traversal().V(root).addE("edge").to(nu).next();
            });
            assertEquals(6L, graph.traversal().V(root).bothE().count().next().longValue());
            IntStream.range(0, 2).forEach(i -> graph.traversal().E().limit(1).drop().iterate());

            assertEquals(4L, graph.traversal().V(root).bothE().count().next().longValue());
        }
    }

    @Test
    public void testFireflyRecordIntegerId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(config);
        FireflyId intId = FireflyId.of(null, 1);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, intId, -1, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, intId);
        assertEquals(record.id(), intId.value());
    }

    @Test
    public void testFireflyRecordLongId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(config);
        FireflyId fid = FireflyId.of(null, 1L);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, fid, -1, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, fid);
        assertEquals(record.id(), fid.value());
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
            db.getClient().put(null, key, new Bin(binName, choices.next()));
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
            db.getClient().put(null, key, bin1, new Bin("weight", 2000 + i), new Bin("age", 32 + i), bin3);
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
    public void testListIndexes() {
        List<String> x = AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace());
        assertTrue(x.contains(db.E_LABEL_INDEX));
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

    //@Ignore
    @Test
    public void testAerospikeInfo() {
        final String binName = "age";
        final String testIndex = "testIndex";
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin3 = new Bin("greeting", "Hello World!");
        int NUMBER_OF_RECORDS = 100;
        IntStream.range(0, NUMBER_OF_RECORDS).forEach(i -> {
            final Key key = new Key(db.getNamespace(), db.TEST_SET, i);
            db.getClient().put(null, key, bin1, new Bin("weight", 2000 + i), new Bin("age", 32 + i), bin3);
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
    public void testFireflyMetadata() throws InterruptedException {
        // Set metadata to update every millisecond for this test.
        db.conf.setProperty(ConfigurationHelper.Keys.ENABLE_PERIODIC_METADATA_UPDATE.toLowerCase(), true);
        db.conf.setProperty(ConfigurationHelper.Keys.METADATA_UPDATE_FREQUENCY.toLowerCase(), "1");
        graph.close();
        graph = FireflyGraph.open(db.conf);

        final Vertex a = graph.traversal().addV("label1").property("key1", "value1").next();
        final Vertex b = graph.traversal().addV("label1").property("key1", "value1").next();
        graph.traversal().addV("label1").property("key2", "value2").iterate();
        graph.traversal().addV("label2").property("key2", "value2").iterate();
        graph.traversal().addV("label2").property("key3", "value3").iterate();
        graph.traversal().addV("label2").property("key3", "value3").iterate();
        graph.traversal().addV("label3").property("key4", "value4").iterate();
        graph.traversal().addV("label3").property("key4", "value4").iterate();
        graph.traversal().addV("label3").property("key5", "value5").iterate();
        graph.traversal().addV("label3").property("key5", "value5").iterate();
        graph.traversal().addV("label3").property("key4", 1).iterate();
        graph.traversal().addV("label3").property("key5", 2).iterate();

        graph.traversal().addE("edgeLabel1").from(a).to(b).property("edgeKey1", "edgeValue1").iterate();
        graph.traversal().addE("edgeLabel1").from(a).to(b).property("edgeKey1", "edgeValue1").iterate();
        graph.traversal().addE("edgeLabel2").from(a).to(b).property("edgeKey2", "edgeValue2").iterate();
        graph.traversal().addE("edgeLabel2").from(a).to(b).property("edgeKey2", "edgeValue2").iterate();
        graph.traversal().addE("edgeLabel3").from(a).to(b).property("edgeKey3", 3).iterate();
        graph.traversal().addE("edgeLabel3").from(a).to(b).property("edgeKey3", 3).iterate();

        // Force an update on the server
        db.dropGraphIndices();
        db.createGraphIndexes();

        Thread.sleep(10);
        final FireflyMetadata.CardinalityInfo vertexLabelCardinalityInfo = FireflyMetadata.vertexLabelCardinalityInfo;
        final FireflyMetadata.CardinalityInfo vertexStringPropertyCardinalityInfo = FireflyMetadata.vertexStringPropertyCardinalityInfo;
        final FireflyMetadata.CardinalityInfo vertexNumericPropertyCardinalityInfo = FireflyMetadata.vertexNumericPropertyCardinalityInfo;
        final FireflyMetadata.CardinalityInfo edgeLabelCardinalityInfo = FireflyMetadata.edgeLabelCardinalityInfo;
        final FireflyMetadata.CardinalityInfo edgeStringPropertyCardinalityInfo = FireflyMetadata.edgeStringPropertyCardinalityInfo;
        final FireflyMetadata.CardinalityInfo edgeNumericPropertyCardinalityInfo = FireflyMetadata.edgeNumericPropertyCardinalityInfo;

        // 12 vertices, 3 unique labels, 5 unique keys, 5 unique string values, 10 total string values, 2 unique numeric values, 5 total numeric values
        Assert.assertTrue(vertexLabelCardinalityInfo.valid);
        Assert.assertTrue(vertexStringPropertyCardinalityInfo.valid);
        Assert.assertTrue(vertexNumericPropertyCardinalityInfo.valid);

        Assert.assertNotNull(vertexLabelCardinalityInfo.totalEntries);
        Assert.assertNotNull(vertexStringPropertyCardinalityInfo.totalEntries);
        Assert.assertNotNull(vertexNumericPropertyCardinalityInfo.totalEntries);

        Assert.assertEquals(12L, vertexLabelCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(10L, vertexStringPropertyCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(2L, vertexNumericPropertyCardinalityInfo.totalEntries.longValue());

        Assert.assertNotNull(vertexLabelCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(vertexStringPropertyCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(vertexNumericPropertyCardinalityInfo.entriesPerBval);

        Assert.assertEquals(12L / 3L, vertexLabelCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(10L / 5L, vertexStringPropertyCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(2L / 2L, vertexNumericPropertyCardinalityInfo.entriesPerBval.longValue());

        // 6 edges, 3 unique labels, 3 unique keys, 2 unique string values, 1 unique numeric values.
        Assert.assertTrue(edgeLabelCardinalityInfo.valid);
        Assert.assertTrue(edgeStringPropertyCardinalityInfo.valid);
        Assert.assertTrue(edgeNumericPropertyCardinalityInfo.valid);

        Assert.assertNotNull(edgeLabelCardinalityInfo.totalEntries);
        Assert.assertNotNull(edgeStringPropertyCardinalityInfo.totalEntries);
        Assert.assertNotNull(edgeNumericPropertyCardinalityInfo.totalEntries);

        Assert.assertEquals(6L, edgeLabelCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(4L, edgeStringPropertyCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(2L, edgeNumericPropertyCardinalityInfo.totalEntries.longValue());

        Assert.assertNotNull(edgeLabelCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(edgeStringPropertyCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(edgeNumericPropertyCardinalityInfo.entriesPerBval);

        Assert.assertEquals(6L / 3L, edgeLabelCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(4L / 2L, edgeStringPropertyCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(2L / 1L, edgeNumericPropertyCardinalityInfo.entriesPerBval.longValue());
    }

    @Test
    public void testAerospikeReadLatency() {
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        IntStream.range(0, 100).forEach(i -> {
            final Key key = new Key(db.getNamespace(), db.TEST_SET, i);
            db.getClient().put(null, key, bin1, bin2, bin3);
        });

        PerfUtil.Results results = PerfUtil.runTestBatch(100, () -> {
            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            final Key key = new Key(db.getNamespace(), db.TEST_SET, tlr.nextInt(0, 100));
            final Record data = db.getClient().get(null, key);
            assert data.getLong("age") == 32;
        });
        System.out.println(results);
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

            // If a value that cannot be parsed to string is added, it should throw an UnsupportedOperationException.
            assertThrows(UnsupportedOperationException.class, () ->
                    g.addV("Mr. T").property(T.id, "can't parse this").iterate());

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
    public void testFireflyEdgeWithUserSuppliedId() {
        // Ids that are strings will be parsed to longs. Integer Ids will be inserted as integers and longs as longs.
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            g.addV("vertex").property("type", "a").
                    addV("vertex").property("type", "b").
                    iterate();

            g.V().has("type", "a").as("a").
                    V().has("type", "b").as("b").
                    addE("user-id-edge").from("a").to("b").property(T.id, 1L).
                    addE("user-id-edge").from("b").to("b").property(T.id, "2").
                    addE("user-id-edge").from("b").to("a").property(T.id, 3).
                    iterate();

            assertEquals(3L, g.E().count().next().longValue());

            // 1L, "2", and 3 were inserted and should be retrieved as such.
            final Set<Edge> actualEdges = g.E().toSet();
            final Set<Object> expectedIds = ImmutableSet.of(1L, "2", 3);
            final Set<Object> actualIds = actualEdges.stream().map(Edge::id).collect(Collectors.toSet());
            assertEquals(expectedIds, actualIds);

            // If a value that cannot be parsed to string is added, it should throw an UnsupportedOperationException.
            assertThrows(UnsupportedOperationException.class, () ->
                    g.addV("vertex").as("a").
                            addV("vertex").as("b").
                            addE("Mr. T").property(T.id, "can't parse this").from("a").to("b").
                            iterate());

            // If a value already exists in the graph then adding it again should throw an IllegalArgumentException.
            // Try adding 1L, "1", and 1, all should fail.
            assertThrows(IllegalArgumentException.class, () ->
                    g.V().has("type", "a").as("a").
                            V().has("type", "b").as("b").
                            addE("user-id-edge").from("a").to("b").property(T.id, 1L).
                            iterate());
            assertThrows(IllegalArgumentException.class, () ->
                    g.V().has("type", "a").as("a").
                            V().has("type", "b").as("b").
                            addE("user-id-edge").from("a").to("b").property(T.id, "1").
                            iterate());
            assertThrows(IllegalArgumentException.class, () ->
                    g.V().has("type", "a").as("a").
                            V().has("type", "b").as("b").
                            addE("user-id-edge").from("a").to("b").property(T.id, 1).
                            iterate());
        }
    }

    @Test
    public void testFireflyVertexIdEdgeIdCollision() {
        // Ids that are strings will be parsed to longs. Integer Ids will be inserted as integers and longs as longs.
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            g.addV("user-id-vertex").property(T.id, "1").
                    addV("user-id-vertex").property(T.id, 2).
                    addV("user-id-vertex").property(T.id, 3L).
                    iterate();
            assertEquals(3L, g.V().count().next().longValue());

            // "1", 2, and 3L were inserted and should be retrieved as such.
            final Set<Vertex> actualVertices = g.V().toSet();
            final Set<Object> expectedVertexIds = ImmutableSet.of("1", 2, 3L);
            final Set<Object> actualVertexIds = actualVertices.stream().map(Vertex::id).collect(Collectors.toSet());
            assertEquals(expectedVertexIds, actualVertexIds);

            g.V().has(T.id, "1").as("a").
                    V().has(T.id, 2).as("b").
                    addE("user-id-edge").from("a").to("b").property(T.id, 1L).
                    addE("user-id-edge").from("b").to("b").property(T.id, "2").
                    addE("user-id-edge").from("b").to("a").property(T.id, 3).
                    iterate();
            assertEquals(3L, g.E().count().next().longValue());

            // 1L, "2", and 3 were inserted and should be retrieved as such.
            final Set<Edge> actualEdges = g.E().toSet();
            final Set<Object> expectedEdgeIds = ImmutableSet.of(1L, "2", 3);
            final Set<Object> actualEdgeIds = actualEdges.stream().map(Edge::id).collect(Collectors.toSet());
            assertEquals(expectedEdgeIds, actualEdgeIds);
        }
    }

    @Test
    public void testDropVerticesEdges() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            assertEquals(0L, g.V().count().next().longValue());
            g.addV("user-id-vertex").property(T.id, "1").
                    addV("user-id-vertex").property(T.id, 2).
                    addV("user-id-vertex").property(T.id, 3L).
                    iterate();
            assertEquals(3L, g.V().count().next().longValue());

            g.V().has(T.id, "1").as("a").
                    V().has(T.id, 2).as("b").
                    addE("user-id-edge").from("a").to("b").property(T.id, 1L).
                    addE("user-id-edge").from("b").to("b").property(T.id, "2").
                    addE("user-id-edge").from("b").to("a").property(T.id, 3).
                    addE("user-id-edge").from("a").to("a").property(T.id, 4).
                    iterate();
            assertEquals(4L, g.E().count().next().longValue());

            // Problem 1: Vertices with string ids are not dropped. properly.
            g.V().has(T.id, 3L).drop().iterate();
            assertEquals(2L, g.V().count().next().longValue());
            assertEquals(4L, g.E().count().next().longValue());

            g.V().has(T.id, "1").drop().iterate();
            assertEquals(1L, g.V().count().next().longValue());
            assertEquals(1L, g.E().count().next().longValue());

            g.V().has(T.id, 2).drop().iterate();
            assertEquals(0L, g.V().count().next().longValue());
            g.E().toList().forEach(e -> System.out.println(e.id()));
            assertEquals(0L, g.E().count().next().longValue());
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
    }

    @Test
    public void shouldRemoveAllData() throws InterruptedException {
        graph.getBaseGraph().dropDatabase();
        AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).forEach(nonEmptySet -> {
            db.getClient().truncate(null, db.getNamespace(), nonEmptySet, null);
        });
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
            while (AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size() == 0)
                sleep(1000);
            graph.traversal().V().drop().iterate();
            sleep(10000);

            // ID_MGR_SET id manager set and G_META graph metadata are not removed by removing all vertices
            Set<String> x = AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient());
            assertEquals(!StarPackedGraph.isStarPackedGraph(graph) ?
                    Set.of("0_G_META") :
                    Set.of("0_IN_IN", "0_G_META", "0_OUT_OUT", "0_OUT_IN", "0_OUT_VP", "0_IN_OUT", "0_IN_VP"),x);
            assertEquals(!StarPackedGraph.isStarPackedGraph(graph) ? 1 : 7, AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size());

            Vertex a = graph.addVertex();
            Vertex b = graph.addVertex();
            Edge e = a.addEdge("edge", b);
            assertEquals(!StarPackedGraph.isStarPackedGraph(graph) ? 4 : 10, AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size());

            graph.traversal().V().drop().iterate();
            sleep(2000);
            Iterator<Map.Entry<Key, Record>> vertxKeys = db.scanAllKeysInSet(db.VERTEX_AERO_SET, null);
            Iterator<Map.Entry<Key, Record>> edgeKeys = db.scanAllKeysInSet(db.EDGE_AERO_SET, null);
            assertFalse(vertxKeys.hasNext());
            assertFalse(edgeKeys.hasNext());


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
}
