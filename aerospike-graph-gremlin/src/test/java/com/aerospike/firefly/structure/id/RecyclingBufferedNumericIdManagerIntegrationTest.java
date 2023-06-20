package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE;

public class RecyclingBufferedNumericIdManagerIntegrationTest {
    private static final long BUFFER_SIZE = 3;
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;
    private byte[] baseline;
    private static final long OFFSET_BASELINE = 1;

    @BeforeClass
    public static void beforeAll() {
        CONFIG.setProperty(EDGE_ID_BUFFER_SIZE.toLowerCase(), String.format("%d", OFFSET_BASELINE));
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        CONFIG.setProperty(EDGE_ID_BUFFER_SIZE.toLowerCase(), String.format("%d", BUFFER_SIZE));
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        this.baseline = getBaselineId();
    }

    private long bytesToLong(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    private byte[] getBaselineId(final long offsetLower, final long offsetUpper) {
        final byte[] lower = new byte[Long.BYTES];
        final byte[] upper = new byte[Long.BYTES];
        System.arraycopy(baseline, 0, lower, 0, Long.BYTES);
        System.arraycopy(baseline, Long.BYTES, upper, 0, Long.BYTES);
        final long lowerLong = bytesToLong(lower) + offsetLower;
        final long upperLong = bytesToLong(upper) + offsetUpper;
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(lowerLong);
        buffer.putLong(upperLong);
        return buffer.array();
    }


    @Test
    public void testBufferedAndUnbufferedIdGet() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final RecyclingBufferedNumericIdManager idManager = (RecyclingBufferedNumericIdManager) graph.edgeIdManager;
            // Buffer -1, -2, -3
            Assert.assertArrayEquals(idManager.getNextId(graph), getBaselineId(-OFFSET_BASELINE - 1, -OFFSET_BASELINE - 1));
            Assert.assertArrayEquals(idManager.getNextId(graph), getBaselineId(-OFFSET_BASELINE - 2, -OFFSET_BASELINE - 2));
            // Buffer -4
            Assert.assertArrayEquals(idManager.getNextId(graph), getBaselineId(-OFFSET_BASELINE - 3, -OFFSET_BASELINE - 3));
            Assert.assertArrayEquals(idManager.getNextId(graph), getBaselineId(-OFFSET_BASELINE - 4, -OFFSET_BASELINE - 4));
        }
    }

    @Test
    public void testIdsReservedProperly() {
        try (final FireflyGraph graph1 = FireflyGraph.open(CONFIG);
             final FireflyGraph graph2 = FireflyGraph.open(CONFIG)) {
            final RecyclingBufferedNumericIdManager idManager1 = (RecyclingBufferedNumericIdManager) graph1.edgeIdManager;
            final RecyclingBufferedNumericIdManager idManager2 = (RecyclingBufferedNumericIdManager) graph2.edgeIdManager;

            Assert.assertArrayEquals(idManager1.getNextId(graph1), getBaselineId(-OFFSET_BASELINE - 1, -OFFSET_BASELINE - 1));
            Assert.assertArrayEquals(idManager2.getNextId(graph2), getBaselineId(-OFFSET_BASELINE - BUFFER_SIZE - 1, -OFFSET_BASELINE - BUFFER_SIZE - 1));
            Assert.assertArrayEquals(idManager1.getNextId(graph1), getBaselineId(-OFFSET_BASELINE - 2, -OFFSET_BASELINE - 2));
            Assert.assertArrayEquals(idManager1.getNextId(graph1), getBaselineId(-OFFSET_BASELINE - 3, -OFFSET_BASELINE - 3));
            Assert.assertArrayEquals(idManager2.getNextId(graph2), getBaselineId(-OFFSET_BASELINE - BUFFER_SIZE - 2, -OFFSET_BASELINE - BUFFER_SIZE - 2));
            Assert.assertArrayEquals(idManager2.getNextId(graph2), getBaselineId(-OFFSET_BASELINE - BUFFER_SIZE - 3, -OFFSET_BASELINE - BUFFER_SIZE - 3));

            // Fourth element of idManager1 will rebuffer and have to go after idManager2.
            Assert.assertArrayEquals(idManager1.getNextId(graph1), getBaselineId(-OFFSET_BASELINE - 2*BUFFER_SIZE - 1, -OFFSET_BASELINE - 2*BUFFER_SIZE - 1));

            // Fourth element of idManager2 will rebuffer and have to go after idManager1.
            Assert.assertArrayEquals(idManager2.getNextId(graph2), getBaselineId(-OFFSET_BASELINE - 3*BUFFER_SIZE - 1, -OFFSET_BASELINE - 3*BUFFER_SIZE - 1));
        }
    }

    @Test
    public void testIdsRecyclePropertly() {
        try (final FireflyGraph graph1 = FireflyGraph.open(CONFIG)) {
            final RecyclingBufferedNumericIdManager idManager1 = (RecyclingBufferedNumericIdManager) graph1.edgeIdManager;

            final byte[] idManager1Id1 = getBaselineId(-OFFSET_BASELINE - 1, -OFFSET_BASELINE - 1);
            final byte[] recycledIdManager1Id1 = getBaselineId(-OFFSET_BASELINE - 1, -OFFSET_BASELINE - 2);
            final byte[] idManager1Id2 = getBaselineId(-OFFSET_BASELINE - 2, -OFFSET_BASELINE - 3);
            final byte[] idManager1Id3 = getBaselineId(-OFFSET_BASELINE - 3, -OFFSET_BASELINE - BUFFER_SIZE - 1);
            final byte[] recycledIdManager1Id2 = getBaselineId(-OFFSET_BASELINE - 2, -OFFSET_BASELINE - BUFFER_SIZE - 2);
            final byte[] recycledIdManager1Id3 = getBaselineId(-OFFSET_BASELINE - 3, -OFFSET_BASELINE - BUFFER_SIZE - 3);


            Assert.assertArrayEquals(idManager1.getNextId(graph1), idManager1Id1);
            idManager1.recycleId(graph1, idManager1Id1);
            Assert.assertArrayEquals(idManager1.getNextId(graph1), recycledIdManager1Id1);
            Assert.assertArrayEquals(idManager1.getNextId(graph1), idManager1Id2);
            Assert.assertArrayEquals(idManager1.getNextId(graph1), idManager1Id3);
            idManager1.recycleId(graph1, idManager1Id2);
            idManager1.recycleId(graph1, idManager1Id3);
            Assert.assertArrayEquals(idManager1.getNextId(graph1), recycledIdManager1Id2);
            Assert.assertArrayEquals(idManager1.getNextId(graph1), recycledIdManager1Id3);
        }
    }

    private static byte[] getBaselineId() {
        // Jog id manager to make sure it re-buffers.
        SETUP_GRAPH.edgeIdManager.getNextId(SETUP_GRAPH);
        return SETUP_GRAPH.edgeIdManager.getNextId(SETUP_GRAPH);
    }
}
