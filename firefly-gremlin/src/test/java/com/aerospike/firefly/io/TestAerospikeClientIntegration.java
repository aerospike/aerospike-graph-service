package com.aerospike.firefly.io;

import com.aerospike.client.*;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.*;
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
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestAerospikeClientIntegration extends AbstractFireflySuite {

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
        FireflyRecord.write(db, db.TEST_SET, FireflyId.of(null, id), bin1, bin2, bin3);
        assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, FireflyId.of(null, id))).record.getInt("age"), 32);
    }

    //@Test
    //public void testBasicReadWriteGetValueOfKey() {
    //    FireflyId id = FireflyId.of(null, "foo");
    //    Bin bin1 = new Bin("name", "John Doe");
    //    Bin bin2 = new Bin("age", 32);
    //    Bin bin3 = new Bin("greeting", "Hello World!");
//
//
    //    Policy rp = new Policy();
    //    rp.sendKey = true;
    //    FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
    //    Key key = db.scanAllRecordsInSet(db.TEST_SET).next().getKey();
    //    assertEquals(Objects.requireNonNull(FireflyRecord.read(db, db.TEST_SET, id)).record.getInt("age"), 32);
    //}

    @Test
    public void testBasicDelete() {
        FireflyId id = FireflyId.of(null, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        FireflyRecord.write(db, db.TEST_SET, id, bin1, bin2, bin3);
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

//    @Test
//    public void testScanVertexIds() {
//        try (FireflyGraph graph = FireflyGraph.open(config)) {
//            ArrayList<Long> ids = new ArrayList<>() {{
//                add(0L);
//                add(1L);
//            }};
//            db.vertexBackend.writeVertex(graph, FireflyId.of(FireflyVertex.class, ids.get(0)), "a");
//            db.vertexBackend.writeVertex(graph, FireflyId.of(FireflyVertex.class, ids.get(1)), "b");
//            Iterator<Long> i = db.scanAllIdsInSet(db.VERTEX_AERO_SET);
//            assertTrue(i.hasNext());
//            Long a = i.next();
//            assertTrue(i.hasNext());
//            assertTrue(ids.contains(a));
//            Long b = i.next();
//            assertTrue(ids.contains(b));
//        }
//    }

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

    //@Test
    //public void testScanEdgeIds() {
    //    try (final FireflyGraph graph = FireflyGraph.open(config)) {
    //        graph.traversal().V().drop().iterate();
    //        ArrayList<Long> vertexIds = new ArrayList<>() {{
    //            add(0L);
    //            add(1L);
    //        }};
    //        ArrayList<Long> edgeIds = new ArrayList<>() {{
    //            add(2L);
    //            add(3L);
    //        }};
    //        db.vertexBackend.writeVertex(graph, FireflyId.of(FireflyVertex.class, vertexIds.get(0)), "a");
    //        FireflyVertex va = db.vertexBackend.readVertex(graph, FireflyId.of(FireflyVertex.class, vertexIds.get(0)));
    //        db.vertexBackend.writeVertex(graph, FireflyId.of(FireflyVertex.class, vertexIds.get(1)), "b");
    //        FireflyVertex vb = db.vertexBackend.readVertex(graph, FireflyId.of(FireflyVertex.class, vertexIds.get(1)));
    //        Iterator<Long> i = db.scanAllIdsInSet(db.VERTEX_AERO_SET);
    //        assertTrue(i.hasNext());
    //        Long a = i.next();
    //        assertTrue(vertexIds.contains(a));
    //        Long b = i.next();
    //        assertTrue(vertexIds.contains(b));
//
    //        db.edgeBackend.writeEdge(graph, FireflyId.of(FireflyEdge.class, edgeIds.get(0)), "anything", va, vb, new Object[]{});
    //        db.edgeBackend.writeEdge(graph, FireflyId.of(FireflyEdge.class, edgeIds.get(1)), "anything", vb, va, new Object[]{});
//
    //        Iterator<Long> ie = db.scanAllIdsInSet(db.EDGE_AERO_SET);
    //        assertTrue(ie.hasNext());
    //        Long ae = ie.next();
    //        assertTrue(edgeIds.contains(ae));
    //        Long be = ie.next();
    //        assertTrue(edgeIds.contains(be));
    //    }
    //}

    // @Test
    // public void testScanQuery() {
    //     FireflyId id1 = FireflyId.of(null, 1L);
    //     Bin bin1 = new Bin("name", "John Doe");
    //     Bin bin2 = new Bin("age", 32);
    //     Bin bin3 = new Bin("greeting", "Hello World!");
    //     FireflyRecord.write(db, db.TEST_SET, id1, bin1, bin2, bin3);
    //     FireflyId id2 = FireflyId.of(null, 2L);
    //     Bin bin21 = new Bin("name", "Jane Doe");
    //     Bin bin22 = new Bin("age", 32);
    //     Bin bin23 = new Bin("greeting", "Hello World!");
    //     FireflyRecord.write(db, db.TEST_SET, id2, bin21, bin22, bin23);
    //     Iterator<Map.Entry<Key, Record>> i = db.scanAllRecordsInSet(db.TEST_SET);
    //     HashMap<Key, Record> results = new HashMap<>();
    //     i.forEachRemaining(entry -> {
    //         results.put(entry.getKey(), entry.getValue());
    //     });
    // }

    @Test
    public void testFireflyRecordIntegerId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(config);
        FireflyId intId = FireflyId.of(null, 1);
        Bin bin21 = new Bin("name", "Jane Doe");
        Bin bin22 = new Bin("age", 32);
        FireflyRecord.write(db, db.TEST_SET, intId, bin21, bin22);
        FireflyRecord record = FireflyRecord.read(db, db.TEST_SET, intId);
        assertEquals(record.id(), intId.value());
    }

    @Test
    public void testFireflyRecordLongId() {
        final String ns = ConfigurationHelper.aerospikeNamespace(config);
        FireflyId fid = FireflyId.of(null, 1L);
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
        db.createIndex(db.TEST_SET, stringIndex, binName, IndexType.STRING, IndexCollectionType.MAPVALUES);
        db.createIndex(db.TEST_SET, numberIndex, binName, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);

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
        db.createIndex(db.TEST_SET, testIndex, binName, IndexType.NUMERIC, IndexCollectionType.DEFAULT);
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

    // @Test
    // public void testCountElements() {
    //     try (final FireflyGraph graph = FireflyGraph.open(config)) {
    //         GraphTraversalSource g = graph.traversal();
    //         g.V().drop().iterate();
    //         ArrayList<Vertex> added = new ArrayList<>();
    //         IntStream.range(0, 10).forEach(i -> {
    //             Vertex nv = graph.addVertex();
    //             added.add(nv);
    //             if (i != 0)
    //                 nv.addEdge("test", added.get(0));
    //         });
    //         assertEquals((long) graph.traversal().E().count().next(), db.edgeBackend.getEdgeCount());
    //         assertEquals((long) graph.traversal().V().count().next(), db.vertexBackend.getVertexCount());
    //     }
    // }

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
        AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).forEach(nonEmptySet -> {
            db.getClient().truncate(null, db.getNamespace(), nonEmptySet, Calendar.getInstance());
        });
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
            while (AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size() == 0)
                sleep(1000);
            graph.traversal().V().drop().iterate();
            sleep(10000);

            // counter set
            assertEquals(1, AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size());

            Vertex a = graph.addVertex();
            Vertex b = graph.addVertex();
            Edge e = a.addEdge("edge", b);
            assertEquals(3, AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size());

            graph.traversal().V().drop().iterate();
            sleep(10000);
            // counter set
            assertEquals(1, AerospikeConnection.InfoOps.getNonEmptySetList(db.getNamespace(), db.getClient()).size());
        }
    }
}
