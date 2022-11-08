package com.aerospike.firefly.process;

import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
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
        SETUP_GRAPH.getBaseGraph().dropDatabase();
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
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

    @After
    public void afterEach() {
        CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
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
        }
    }

    @Test
    public void testDropStrategyWithEdgeDrop() {
        if (SETUP_GRAPH.getDataModel().equals(StarPackedGraph.DATA_MODEL)) {
            // TODO: Not sure how robust this is long-term and if it needs to be addressed.
            //       A stray edge is the only way to check whether or not the database was dropped versus an iterated
            //       removal of elements via a traversal. In the StarPacked model, trying to remove compound edges
            //       chained off of the removal of the stray edge causes exceptions due to the stray edge not having
            //       valid vertices attached to it which would not be possible when an edge is inserted through the
            //       proper interfaces. Skipping this test is "safe" since we're testing whether or not the strategy
            //       applies given a traversal pattern, so as long as we know the strategy is registered to this model,
            //       which we do from the other tests on this model, and we know that the pattern matches, which we do
            //       from this test on other models, we can transitively say this is functional.
            return;
        }
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
}
