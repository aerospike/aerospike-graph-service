package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
import static org.junit.Assert.fail;

public class TestFireflyDropStrategyIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        SETUP_GRAPH.close();
    }

    private static byte[] getEdgeId() {
        final byte[] buffer = new byte[16];
        new Random().nextBytes(buffer);
        return buffer;
    }

    @Before
    public void beforeEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        List<Map.Entry<String, Object>> properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Simon"));
        final FireflyVertex simon = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(1), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Lyndon"));
        final FireflyVertex lyndon = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(2), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Vincent"));
        final FireflyVertex vincent = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(3), "cat", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Mycroft"));
        final FireflyVertex mycroft = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(4), "cat", properties);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "coworker", Collections.emptyList(),
                lyndon, simon);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "coworker", Collections.emptyList(),
                simon, lyndon);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "owns", Collections.emptyList(),
                vincent, lyndon);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "owns", Collections.emptyList(),
                mycroft, lyndon);
        // Write a stray edge that normal drop traversal would not remove
        SETUP_GRAPH.bulkWriteEdge(getEdgeId(), "stray", Collections.emptyList(), 5, 5, false, false);
    }

    @After
    public void afterEach() {
        CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
    }

    @Test
    public void testDropStrategyDefaultIterate() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabledIterate(graph);
        }
    }

    @Test
    public void testDropStrategyEnabledIterate() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabledIterate(graph);
        }
    }

    @Test
    public void testDropStrategyDefaultToList() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabledToList(graph);
        }
    }

    @Test
    public void testDropStrategyEnabledToList() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabledToList(graph);
        }
    }

    @Test
    public void testDropStrategyDefaultNext() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabledNext(graph);
        }
    }

    @Test
    public void testDropStrategyEnabledNext() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabledNext(graph);
        }
    }

    @Test
    public void testDropStrategyWithVertexLabelFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().hasLabel("cat").drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(2, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(3, edgeCount);
        }
    }

    @Test
    public void testDropStrategyWithVertexPropertyFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().has("name", "Simon").drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(3, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(3, edgeCount);
        }
    }

    @Test
    public void testDropStrategyWithVertexIdFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().hasId(1).drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(3, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(3, edgeCount);
        }
    }

    @Test
    public void testDropStrategyWithVertexFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V(1, 3).drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(2, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(2, edgeCount);
        }
    }

    @Test
    public void testDropStrategyWithEdgeDrop() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.E().drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(0, edgeCount);
        }
    }

    @Test
    public void testDropStrategyDisabledIterate() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(0, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(1, edgeCount);
        }
    }

    @Test
    public void testDropStrategyDisabledToList() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().drop().toList();
            vertexCount = g.V().count().next();
            Assert.assertEquals(0, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(1, edgeCount);
        }
    }

    @Test
    public void testDropStrategyDisabledNext() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();

            // Ensure arbitrary unterminated call to g.V().drop() does nothing.
            g.V().drop();

            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            try {
                g.V().drop().next();
                fail("Should throw NoSuchElementException on g.V().drop().next().");
            } catch (final NoSuchElementException e) {
                // This is expected since traversal output is empty
                // however it should still remove the data.
            }
            vertexCount = g.V().count().next();
            Assert.assertEquals(0, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(1, edgeCount);
        }
    }

    private void assertDropStrategyEnabledIterate(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();

        // Ensure arbitrary unterminated call to g.V().drop() does nothing.
        g.V().drop();

        long vertexCount = g.V().count().next();
        Assert.assertEquals(4, vertexCount);
        long edgeCount = g.E().count().next();
        Assert.assertEquals(5, edgeCount);
        g.V().drop().iterate();
        vertexCount = g.V().count().next();
        Assert.assertEquals(0, vertexCount);
        edgeCount = g.E().count().next();
        Assert.assertEquals(0, edgeCount);
    }

    private void assertDropStrategyEnabledNext(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();

        // Ensure arbitrary unterminated call to g.V().drop() does nothing.
        g.V().drop();

        long vertexCount = g.V().count().next();
        Assert.assertEquals(4, vertexCount);
        long edgeCount = g.E().count().next();
        Assert.assertEquals(5, edgeCount);
        try {
            g.V().drop().next();
            fail("Should throw NoSuchElementException on next().");
        } catch (final NoSuchElementException e) {
            // This is expected since traversal output is empty
            // however it should still remove the data.
        }
        vertexCount = g.V().count().next();
        Assert.assertEquals(0, vertexCount);
        edgeCount = g.E().count().next();
        Assert.assertEquals(0, edgeCount);
    }

    private void assertDropStrategyEnabledToList(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();

        // Ensure arbitrary unterminated call to g.V().drop() does nothing.
        g.V().drop();

        long vertexCount = g.V().count().next();
        Assert.assertEquals(4, vertexCount);
        long edgeCount = g.E().count().next();
        Assert.assertEquals(5, edgeCount);
        g.V().drop().toList();
        vertexCount = g.V().count().next();
        Assert.assertEquals(0, vertexCount);
        edgeCount = g.E().count().next();
        Assert.assertEquals(0, edgeCount);
    }
}
