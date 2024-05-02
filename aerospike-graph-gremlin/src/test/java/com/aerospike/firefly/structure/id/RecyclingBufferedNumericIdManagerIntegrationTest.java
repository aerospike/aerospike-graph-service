package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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
    public void testIdsRecycleProperly() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v1 = g.addV("one").next();
            final Vertex v2 = g.addV("two").next();
            // Ensure use sizing greater than the edge pack size to ensure not just current edge pack is refilled
            final int testSize = (graph.getBaseGraph().PHAT_EDGE_SIZE * 2) + 2;
            // Automatic ID generation is decrementing so start with the highest value to ensure sequential packing IDs
            long highestId = Long.MIN_VALUE;
            final Set<String> originalEdgeIds = new HashSet<>();
            final Set<Long> packingIds = new HashSet<>();
            for (int i = 0; i < testSize; i++) {
                final Edge e = g.addE("edge").from(v1).to(v2).next();
                final String id = (String) e.id();
                originalEdgeIds.add(id);
                final Long packingId = edgeIdToPackingIdLong(id);
                highestId = packingId >= highestId ? packingId : highestId;
                packingIds.add(packingId);
            }
            Assert.assertEquals(testSize, originalEdgeIds.size());
            Assert.assertEquals(testSize, packingIds.size());
            // Ensure the IDs are sequential as a baseline
            for (long i = 0; i < testSize; i++) {
                Assert.assertTrue(packingIds.contains(highestId - i));
            }

            // Pick 2 random edges to drop
            final Random rng = new Random();
            int item1 = rng.nextInt(originalEdgeIds.size());
            int item2 = rng.nextInt(originalEdgeIds.size());
            // Ensure edges aren't the same
            while (item1 == item2) {
                item2 = rng.nextInt(originalEdgeIds.size());
            }

            // Drop the 2 edges and record their packing IDs
            final Set<Long> removedPackedIds = new HashSet<>();
            int i = 0;
            for (final String edgeId : originalEdgeIds) {
                if (i == item1 || i == item2) {
                    g.E(edgeId).drop().iterate();
                    removedPackedIds.add(edgeIdToPackingIdLong(edgeId));
                }
                i++;
            }

            // Add two new edges and ensure they recycle the packing IDs.
            final Edge e1 = g.addE("edge").from(v1).to(v2).next();
            final Edge e2 = g.addE("edge").from(v1).to(v2).next();
            Assert.assertFalse(originalEdgeIds.contains((String) e1.id()));
            Assert.assertFalse(originalEdgeIds.contains((String) e2.id()));
            Assert.assertTrue(removedPackedIds.contains(edgeIdToPackingIdLong((String) e1.id())));
            Assert.assertTrue(removedPackedIds.contains(edgeIdToPackingIdLong((String) e2.id())));
        }
    }

    @Test
    public void testCompositeIdRecycle() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v1 = g.addV("one").next();
            final Vertex v2 = g.addV("two").next();

            final Set<Long> packingIds = new HashSet<>();
            final Edge e1 = v1.addEdge("edge", v2);
            final Edge e2 = v2.addEdge("edge", v1);
            packingIds.add(edgeIdToPackingIdLong((String) e1.id()));
            packingIds.add(edgeIdToPackingIdLong((String) e2.id()));
            v1.edges(Direction.BOTH).forEachRemaining(e -> e.remove());
            Assert.assertFalse(g.E(e1.id()).hasNext());
            Assert.assertFalse(g.E(e2.id()).hasNext());
            final Edge e3 = v1.addEdge("edge", v2);
            final Edge e4 = v1.addEdge("edge", v1);
            Assert.assertTrue(packingIds.contains(edgeIdToPackingIdLong((String) e3.id())));
            Assert.assertTrue(packingIds.contains(edgeIdToPackingIdLong((String) e4.id())));
        }
    }

    @Test
    public void testNoDuplicateRecyclingDueToDuplicateEdgeDrop() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final FireflyGraph graph = FireflyGraph.open(config);
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        final FireflyEdge e1 = (FireflyEdge) g.addE("e1").from(v1).to(v2).next();
        final FireflyEdge e2 = (FireflyEdge) g.addE("e2").from(v1).to(v2).next();

        final RecyclingBufferedNumericIdManager edgeIdManager = (RecyclingBufferedNumericIdManager) graph.edgeIdManager;
        Assert.assertEquals(0, edgeIdManager.availableRecycledIds());
        // Test removal within phat edge (record persists after removal of edge)
        e1.removeEdge();
        e1.removeEdge();
        Assert.assertEquals(1, edgeIdManager.availableRecycledIds());
        Assert.assertFalse(g.E(e1.id()).hasNext());
        // Test removal of last edge in phat edge (record deleted after removal of edge)
        e2.removeEdge();
        e2.removeEdge();
        Assert.assertEquals(2, edgeIdManager.availableRecycledIds());
        Assert.assertFalse(g.E(e2.id()).hasNext());
    }

    private static byte[] getBaselineId() {
        // Jog id manager to make sure it re-buffers.
        SETUP_GRAPH.edgeIdManager.getNextId(SETUP_GRAPH);
        return SETUP_GRAPH.edgeIdManager.getNextId(SETUP_GRAPH);
    }

    private static Long edgeIdToPackingIdLong(final String edgeId) {
        final byte[] fullIdAsByteArray = Base64.getDecoder().decode(edgeId);
        final byte[] packingIdAsByteArray = ArrayUtils.subarray(fullIdAsByteArray, 0, 8);
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(packingIdAsByteArray);
        buffer.flip();
        return buffer.getLong();
    }
}
