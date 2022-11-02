package com.aerospike.firefly.structure.process;

import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphDropStrategy;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;

public class TestFireflyDropStrategyIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        removeStrategyFromGlobalCache();
        SETUP_GRAPH.getBaseGraph().dropDatabase();
        List<Map.Entry<String, Object>> properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Simon"));
        final FireflyVertex simon = SETUP_GRAPH.writeVertex(FireflyId.of(FireflyVertex.class, 1), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Lyndon"));
        final FireflyVertex lyndon = SETUP_GRAPH.writeVertex(FireflyId.of(FireflyVertex.class, 2), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Vincent"));
        final FireflyVertex vincent = SETUP_GRAPH.writeVertex(FireflyId.of(FireflyVertex.class, 3), "cat", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Mycroft"));
        final FireflyVertex mycroft = SETUP_GRAPH.writeVertex(FireflyId.of(FireflyVertex.class, 4), "cat", properties);
        SETUP_GRAPH.writeEdge(FireflyId.of(FireflyEdge.class, 1), "coworker", Collections.emptyList(),
                lyndon, simon);
        SETUP_GRAPH.writeEdge(FireflyId.of(FireflyEdge.class, 2), "coworker", Collections.emptyList(),
                simon, lyndon);
        SETUP_GRAPH.writeEdge(FireflyId.of(FireflyEdge.class, 3), "owns", Collections.emptyList(),
                vincent, lyndon);
        SETUP_GRAPH.writeEdge(FireflyId.of(FireflyEdge.class, 4), "owns", Collections.emptyList(),
                mycroft, lyndon);
        // Write a stray edge that normal drop traversal would not remove
        SETUP_GRAPH.bulkWriteEdge(5, "stray", Collections.emptyList(), 5, 5);
    }

    @Test
    public void testDropStrategyDefault() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabled(graph);
        }
    }

    @Test
    public void testDropStrategyEnabled() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertDropStrategyEnabled(graph);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void testDropStrategyWithVertexLabelFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().hasLabel("cat").drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(2, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(3, edgeCount);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void testDropStrategyWithVertexPropertyFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().has("name", "Simon").drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(3, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(3, edgeCount);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void testDropStrategyWithVertexIdFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().hasId(1).drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(3, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(3, edgeCount);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void testDropStrategyWithVertexFilter() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V(1, 3).drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(2, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(2, edgeCount);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void testDropStrategyWithEdgeDrop() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.E().drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(0, edgeCount);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    @Test
    public void testDropStrategyDisabled() {
        CONFIG.setProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            long vertexCount = g.V().count().next();
            Assert.assertEquals(4, vertexCount);
            long edgeCount = g.E().count().next();
            Assert.assertEquals(5, edgeCount);
            g.V().drop().iterate();
            vertexCount = g.V().count().next();
            Assert.assertEquals(0, vertexCount);
            edgeCount = g.E().count().next();
            Assert.assertEquals(1, edgeCount);
        } finally {
            CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
        }
    }

    private void assertDropStrategyEnabled(FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
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

    private void removeStrategyFromGlobalCache() {
        TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class)
                .removeStrategies(FireflyGraphDropStrategy.class);
    }
}
