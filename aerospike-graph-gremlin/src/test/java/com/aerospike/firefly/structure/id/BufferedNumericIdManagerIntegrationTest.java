package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.VERTEX_ID_BUFFER_SIZE;

public class BufferedNumericIdManagerIntegrationTest {
    private static final String BUFFER_SIZE = "3";
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;
    private long baseline;

    @BeforeClass
    public static void beforeAll() {
        CONFIG.setProperty(VERTEX_ID_BUFFER_SIZE.toLowerCase(), "1");
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        CONFIG.setProperty(VERTEX_ID_BUFFER_SIZE.toLowerCase(), BUFFER_SIZE);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        this.baseline = getBaselineId();
    }

    @Test
    public void testBufferedAndUnbufferedIdGet() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final BufferedNumericIdManager idManager = (BufferedNumericIdManager) graph.getIdFactory().getVertexIdManager();
            // Buffer -1, -2, -3
            Assert.assertEquals(idManager.getNextId(graph).longValue(), baseline-1);
            Assert.assertEquals(idManager.getNextId(graph).longValue(), baseline-2);
            // Buffer -4
            Assert.assertEquals(idManager.getNextId(graph).longValue(), baseline-3);
            Assert.assertEquals(idManager.getNextId(graph).longValue(), baseline-4);
        }
    }

    @Test
    public void testIdsReservedProperly() {
        try (final FireflyGraph graph1 = FireflyGraph.open(CONFIG);
             final FireflyGraph graph2 = FireflyGraph.open(CONFIG)) {
            final BufferedNumericIdManager idManager1 = (BufferedNumericIdManager) graph1.getIdFactory().getVertexIdManager();
            final BufferedNumericIdManager idManager2 = (BufferedNumericIdManager) graph2.getIdFactory().getVertexIdManager();

            // Buffer -1, -2, -3
            Assert.assertEquals(idManager1.getNextId(graph1).longValue(), baseline-1);
            // Buffer -4, -5, -6
            Assert.assertEquals(idManager2.getNextId(graph2).longValue(), baseline-4);
            Assert.assertEquals(idManager2.getNextId(graph2).longValue(), baseline-5);
            Assert.assertEquals(idManager1.getNextId(graph1).longValue(), baseline-2);
            // Buffer -7, -8, -9
            Assert.assertEquals(idManager1.getNextId(graph1).longValue(), baseline-3);
            Assert.assertEquals(idManager1.getNextId(graph1).longValue(), baseline-7);
            // Buffer -10, -11, -12
            Assert.assertEquals(idManager2.getNextId(graph2).longValue(), baseline-6);
            Assert.assertEquals(idManager2.getNextId(graph2).longValue(), baseline-10);
        }
    }

    private static long getBaselineId() {
        // Jog id manager to make sure it re-buffers.
        SETUP_GRAPH.getIdFactory().getVertexIdManager().getNextId(SETUP_GRAPH);
        return SETUP_GRAPH.getIdFactory().getVertexIdManager().getNextId(SETUP_GRAPH) - 1;
    }
}
