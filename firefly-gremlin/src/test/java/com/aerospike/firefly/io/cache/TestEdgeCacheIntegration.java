package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestEdgeCacheIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = CacheTestsUtils.getCacheDefaultFirefly(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase();
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase();
    }

    @Test
    public void testCacheEnabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheDefaultFirefly(CONFIG)) {
            final FireflyTestVertexes vertexes = setupTest(graph);
            assertGremlinTraversalAccuracy(graph);

            // Since the cache is enabled, the instantiated vertexes will use their internal cache to return edges,
            // which are empty since the edges were added to Aerospike via a traversal after their instantiation
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.IN), 0);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.IN), 0);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.IN), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.IN), 0);
        }
    }

    @Test
    public void testCacheDisabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getEdgeCacheDisabledFirefly(CONFIG)) {
            final FireflyTestVertexes vertexes = setupTest(graph);
            assertGremlinTraversalAccuracy(graph);

            // Since the cache is disabled, the instantiated vertexes will query Aerospike to return edges,
            // which will return the correct values even if they do not exist in the instantiated JVM edge caches
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.OUT), 3);
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.IN), 2);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.OUT), 1);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.IN), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.OUT), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.IN), 1);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.IN), 1);
        }
    }

    @Test
    public void testCacheAndAdjacencyIndexDisabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getEdgeAdjacencyIndexDisabledFirefly(CONFIG)) {
            final FireflyTestVertexes vertexes = setupTest(graph);
            assertGremlinTraversalAccuracy(graph);

            // Since the cache is disabled, the instantiated vertexes will query Aerospike to return edges,
            // which will return the correct values even if they do not exist in the instantiated JVM edge caches
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.OUT), 3);
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.IN), 2);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.OUT), 1);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.IN), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.OUT), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.IN), 1);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.IN), 1);
        }
    }

    @Test
    public void testCacheLimit() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheWithSizeFirefly(CONFIG, 2)) {
            // This time specifically returns referenced instances and not snapshot instantiated ones
            final FireflyTestVertexes vertexes = setupTest(graph, false);
            assertGremlinTraversalAccuracy(graph);

            // The cache will not be disabled for zeroOutOneIn, since it has a total edge count of < 2
            Assert.assertTrue(vertexes.threeOutTwoIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.OUT), 3);
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.IN), 2);
            Assert.assertTrue(vertexes.oneOutTwoIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.OUT), 1);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.IN), 2);
            Assert.assertTrue(vertexes.twoOutOneIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.OUT), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.IN), 1);
            Assert.assertFalse(vertexes.zeroOutOneIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.IN), 1);
        }
    }

    private void assertGremlinTraversalAccuracy(final FireflyGraph graph) {
        // This should be consistent across all cache states
        final GraphTraversalSource g = graph.traversal();
        assertEdgeCount(g.V().hasLabel("threeOutTwoIn").outE(), 3);
        assertEdgeCount(g.V().hasLabel("threeOutTwoIn").inE(), 2);
        assertEdgeCount(g.V().hasLabel("oneOutTwoIn").outE(), 1);
        assertEdgeCount(g.V().hasLabel("oneOutTwoIn").inE(), 2);
        assertEdgeCount(g.V().hasLabel("twoOutOneIn").outE(), 2);
        assertEdgeCount(g.V().hasLabel("twoOutOneIn").inE(), 1);
        assertEdgeCount(g.V().hasLabel("zeroOutOneIn").outE(), 0);
        assertEdgeCount(g.V().hasLabel("zeroOutOneIn").inE(), 1);
    }

    private FireflyTestVertexes setupTest(final FireflyGraph graph, final boolean returnSnapshot) {
        graph.getBaseGraph().dropDatabase();
        final GraphTraversalSource g = graph.traversal();
        final FireflyVertex threeOutTwoInSnapshot = (FireflyVertex) g.addV("threeOutTwoIn").next();
        final FireflyVertex oneOutTwoInSnapshot = (FireflyVertex) g.addV("oneOutTwoIn").next();
        final FireflyVertex twoOutOneInSnapshot = (FireflyVertex) g.addV("twoOutOneIn").next();
        final FireflyVertex zeroOutOneInSnapshot = (FireflyVertex) g.addV("zeroOutOneIn").next();

        final Vertex threeOutTwoIn = g.V().hasLabel("threeOutTwoIn").next();
        final Vertex oneOutTwoIn = g.V().hasLabel("oneOutTwoIn").next();
        final Vertex twoOutOneIn = g.V().hasLabel("twoOutOneIn").next();
        final Vertex zeroOutOneIn = g.V().hasLabel("zeroOutOneIn").next();
        g.addE("edge").from(threeOutTwoIn).to(oneOutTwoIn).iterate();
        g.addE("edge").from(threeOutTwoIn).to(twoOutOneIn).iterate();
        g.addE("edge").from(threeOutTwoIn).to(zeroOutOneIn).iterate();
        g.addE("edge").from(oneOutTwoIn).to(threeOutTwoIn).iterate();
        g.addE("edge").from(twoOutOneIn).to(threeOutTwoIn).iterate();
        g.addE("edge").from(twoOutOneIn).to(oneOutTwoIn).iterate();

        if (returnSnapshot) {
            return new FireflyTestVertexes(threeOutTwoInSnapshot, oneOutTwoInSnapshot,
                    twoOutOneInSnapshot, zeroOutOneInSnapshot);
        } else {
            return new FireflyTestVertexes((FireflyVertex) threeOutTwoIn, (FireflyVertex) oneOutTwoIn,
                    (FireflyVertex) twoOutOneIn, (FireflyVertex) zeroOutOneIn);
        }
    }

    private FireflyTestVertexes setupTest(final FireflyGraph graph) {
        return setupTest(graph, true);
    }

    private void assertEdgeCount(Iterator<Edge> edges, final int expected) {
        int amount = 0;
        while (edges.hasNext()) {
            amount++;
            edges.next();
        }
        Assert.assertEquals(amount, expected);
    }

    private static class FireflyTestVertexes {
        private final FireflyVertex threeOutTwoIn;
        private final FireflyVertex oneOutTwoIn;
        private final FireflyVertex twoOutOneIn;
        private final FireflyVertex zeroOutOneIn;

        private FireflyTestVertexes(final FireflyVertex threeOutTwoIn, final FireflyVertex oneOutTwoIn,
                                    final FireflyVertex twoOutOneIn, final FireflyVertex zeroOutOneIn) {
            this.threeOutTwoIn = threeOutTwoIn;
            this.oneOutTwoIn = oneOutTwoIn;
            this.twoOutOneIn = twoOutOneIn;
            this.zeroOutOneIn = zeroOutOneIn;
        }
    }
}
