package com.aerospike.firefly.olap;


import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import io.vavr.collection.Stream;
import org.apache.commons.configuration2.Configuration;
import org.javatuples.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDistributedAerospikeConnection {
    private static FireflyGraph graph;
    private static AerospikeConnection db;

    @BeforeClass
    public static void setUp() {
        final Configuration config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        graph = FireflyGraph.open(config);
        db = graph.getBaseGraph();
    }

    @AfterClass
    public static void tearDown() {
        graph.close();
    }

    @Before
    public void beforeEach() {
        db.truncate(null, db.OLAP_SET, null);
    }

    @Test
    public void testAccumulatorPacking() {
        final long elementCount = 10_000;
        final long packSize = 10;
        final DistributedAerospikeConnection ddb =
                new DistributedAerospikeConnection(db, db.getNamespace(), db.OLAP_SET, elementCount, packSize);
        final List<String> ids = Stream.range(1, 10_001).map(String::valueOf).collect(Collectors.toList());
        assertEquals(10_000, ids.size());
        for (final String id : ids) {
            ddb.addPackedAccumulatorDouble(id, 0.0, 0);
        }
        final Queue<com.aerospike.client.Record> records = new ConcurrentLinkedQueue<>();
        db.scanAll(null, db.OLAP_SET, (key, record) -> records.add(record));
        assertEquals(1000, records.size());
    }

    @Test
    public void testAccumulatorIteration() {
        final long elementCount = 10_000;
        final long packSize = 10;
        final DistributedAerospikeConnection ddb =
                new DistributedAerospikeConnection(db, db.getNamespace(), db.OLAP_SET, elementCount, packSize);
        final List<String> ids = Stream.range(1, 10_001).map(String::valueOf).collect(Collectors.toList());
        assertEquals(10_000, ids.size());
        for (final String id : ids) {
            ddb.addPackedAccumulatorDouble(id, 0.0, 0);
            ddb.addPackedAccumulatorDouble(id, 3.1, 0);
            ddb.addPackedAccumulatorDouble(id, 1.0, 1);
            ddb.addPackedAccumulatorDouble(id, 1.5, 1);
        }
        for (final String id : ids) {
            assertEquals((Double) 3.1, ddb.getPackedAccumulatorDouble(id, 0));
            assertEquals((Double) 2.5, ddb.getPackedAccumulatorDouble(id, 1));
        }
    }

    @Test
    public void testMinValue() {
        final long elementCount = 10_000;
        final long packSize = 10;
        final DistributedAerospikeConnection ddb =
                new DistributedAerospikeConnection(db, db.getNamespace(), db.OLAP_SET, elementCount, packSize);
        ddb.setPackedMinValue("foo", "5000");
        final String value = ddb.getPackedMinValue("foo");
        assertEquals("5000", value);
        ddb.setPackedMinValue("foo", "5001");
        final String value2 = ddb.getPackedMinValue("foo");
        assertEquals("5000", value2);
        ddb.setPackedMinValue("foo", "4000");
        final String value3 = ddb.getPackedMinValue("foo");
        assertEquals("4000", value3);
    }

    @Test
    public void testMinValuePacking() {
        final long elementCount = 10_000;
        final long packSize = 10;
        final DistributedAerospikeConnection ddb =
                new DistributedAerospikeConnection(db, db.getNamespace(), db.OLAP_SET, elementCount, packSize);
        final List<String> ids = Stream.range(1, 10_001).map(String::valueOf).collect(Collectors.toList());
        assertEquals(10_000, ids.size());
        for (final String id : ids) {
            ddb.addPackedAccumulatorDouble(id, 1.5, 1);
        }
        final Queue<com.aerospike.client.Record> records = new ConcurrentLinkedQueue<>();
        db.scanAll(null, db.OLAP_SET, (key, record) -> records.add(record));
        assertEquals(1000, records.size());
    }

    @Test
    public void testPairs() {
        final DistributedAerospikeConnection ddb = new DistributedAerospikeConnection(db, 100, 10);

        for (Integer i = 0; i < 10; i++) {
            ddb.addPair("test", i.toString(), i.doubleValue(), 1);
        }

        final List<?> result = ddb.getPairs("test", 1);
        assertEquals(10, result.size());

        final Pair<?,?> pair5 = (Pair<?,?>) result.get(5);
        assertEquals("5", pair5.getValue0());
        assertEquals(5.0, pair5.getValue1());
    }

    @Test
    public void testPackedMaxPairs() {
        final DistributedAerospikeConnection ddb = new DistributedAerospikeConnection(db, 100, 10);

        // create new
        ddb.setPackedPairMax("test", "a", 1.0, 1);
        Pair<String, Double> result = ddb.getPackedPairMax("test", 1);
        assertEquals("a", result.getValue0());
        assertEquals(1.0D, result.getValue1(), 0.000001);

        // do not change because smaller value
        ddb.setPackedPairMax("test", "b", 0.0, 1);
        result = ddb.getPackedPairMax("test", 1);
        assertEquals("a", result.getValue0());
        assertEquals(1.0D, result.getValue1(), 0.000001);

        // do change because bigger value
        ddb.setPackedPairMax("test", "c", 2.0, 1);
        result = ddb.getPackedPairMax("test", 1);
        assertEquals("c", result.getValue0());
        assertEquals(2.0D, result.getValue1(), 0.000001);

        // do change because same value and bigger name
        ddb.setPackedPairMax("test", "d", 2.0, 1);
        result = ddb.getPackedPairMax("test", 1);
        assertEquals("d", result.getValue0());
        assertEquals(2.0D, result.getValue1(), 0.000001);
    }

    @Test
    public void testPackedPairs() {
        final DistributedAerospikeConnection ddb = new DistributedAerospikeConnection(db, 100, 10);

        // run on empty
        List<Pair<String, Double>> pairs = ddb.getPackedPairs("test", 1);
        assertTrue(pairs.isEmpty());

        ddb.addPackedPair("test", "a", 1.0, 1);
        pairs = ddb.getPackedPairs("test", 1);
        assertEquals(1, pairs.size());

        ddb.addPackedPair("test", "a", 2.0, 1);
        pairs = ddb.getPackedPairs("test", 1);
        assertEquals(2, pairs.size());

        ddb.addPackedPair("test", "b", 2.0, 1);
        pairs = ddb.getPackedPairs("test", 1);
        assertEquals(3, pairs.size());
        assertEquals("a", pairs.get(0).getValue0());
        assertEquals(1.0, pairs.get(0).getValue1(), 0.000001);
    }
}