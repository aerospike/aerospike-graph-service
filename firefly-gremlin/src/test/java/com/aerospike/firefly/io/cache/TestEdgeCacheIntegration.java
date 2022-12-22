package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
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

            final FireflyVertex v1 = (FireflyVertex) graph.traversal().addV("v1").next();
            final FireflyVertex v2 = (FireflyVertex) graph.traversal().addV("v2").next();
            Assert.assertTrue(v1.isEdgeCacheDisabled());
            Assert.assertTrue(v2.isEdgeCacheDisabled());
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

            // The cache will only be disabled for threeOutTwoIn, since it has 3 OUT edges which exceeds cache size of 2
            Assert.assertTrue(vertexes.threeOutTwoIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.OUT), 3);
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.IN), 2);
            Assert.assertFalse(vertexes.oneOutTwoIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.OUT), 1);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.IN), 2);
            Assert.assertFalse(vertexes.twoOutOneIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.OUT), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.IN), 1);
            Assert.assertFalse(vertexes.zeroOutOneIn.isEdgeCacheDisabled());
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.IN), 1);
        }
    }

    @Test
    public void testAddAndRemoveEdgesAdjacencyIndexEnabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheWithSizeFirefly(CONFIG, 3)) {
            testAddAndRemoveEdges(graph);
        }
    }

    @Test
    public void testAddAndRemoveEdgesAdjacencyIndexDisabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheWithSizeFireflyAdjacencyDisabled(CONFIG, 3)) {
            testAddAndRemoveEdges(graph);
        }
    }

    private void testAddAndRemoveEdges(final FireflyGraph graph) {
        graph.getBaseGraph().dropDatabase();

        // This tests adding and removal of edges, in states where the cache is enabled and disabled, as well as the
        // cache state being triggered to disable.
        final GraphTraversalSource g = graph.traversal();
        FireflyVertex v1 = (FireflyVertex) g.addV("v1").next();
        FireflyVertex v2 = (FireflyVertex) g.addV("v2").next();
        FireflyVertex v3 = (FireflyVertex) g.addV("v3").next();
        Assert.assertFalse(v1.isEdgeCacheDisabled());
        Assert.assertFalse(v2.isEdgeCacheDisabled());
        Assert.assertFalse(v3.isEdgeCacheDisabled());

        // 3 OUT shouldn't disable cache.
        g.addE("v1Out1").from(v1).to(v2).next();
        g.addE("v1Out2").from(v1).to(v2).next();
        g.addE("v1Out3").from(v1).to(v2).next();
        Assert.assertFalse(v1.isEdgeCacheDisabled());
        Assert.assertFalse(v2.isEdgeCacheDisabled());
        Assert.assertEquals(3, IteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(0, IteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertEquals(0, IteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(3, IteratorUtils.count(v2.edges(Direction.IN)));

        // Ensure OUT and IN counts (4 total) are separate for triggering cache disable.
        g.addE("v1In1").from(v3).to(v1).next();
        Assert.assertEquals(3, IteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, IteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertFalse(v1.isEdgeCacheDisabled());
        Assert.assertEquals(1, IteratorUtils.count(v3.edges(Direction.OUT)));
        Assert.assertEquals(0, IteratorUtils.count(v3.edges(Direction.IN)));
        Assert.assertFalse(v3.isEdgeCacheDisabled());

        // Check edge removal when cache is enabled still.
        Assert.assertTrue(g.V().hasLabel("v1").outE("v1Out3").hasNext());
        Assert.assertTrue(g.V().hasLabel("v2").inE("v1Out3").hasNext());
        g.E().hasLabel("v1Out3").drop().iterate();
        // Need to grab the vertexes again to refresh its state in the JVM cache
        v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(2, IteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, IteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertFalse(v1.isEdgeCacheDisabled());
        Assert.assertEquals(0, IteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(2, IteratorUtils.count(v2.edges(Direction.IN)));
        Assert.assertFalse(v2.isEdgeCacheDisabled());
        Assert.assertFalse(g.V().hasLabel("v1").outE("v1Out3").hasNext());
        Assert.assertFalse(g.V().hasLabel("v2").inE("v1Out3").hasNext());

        // Disable the cache for v1.
        g.addE("v1Out3").from(v1).to(v2).next();
        // Add to v3 here to ensure cache counter logic is properly decoupled between both ends of the edge.
        g.addE("v1Out4").from(v1).to(v3).next();
        Assert.assertTrue(v1.isEdgeCacheDisabled());
        // This also implicitly ensures the drop of the edge on v2 earlier properly decremented its edge count.
        Assert.assertFalse(v2.isEdgeCacheDisabled());
        Assert.assertFalse(v3.isEdgeCacheDisabled());
        // Sanity check on generated vertices on reading from database.
        v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        v3 = (FireflyVertex) g.V().hasLabel("v3").next();
        Assert.assertTrue(v1.isEdgeCacheDisabled());
        Assert.assertFalse(v2.isEdgeCacheDisabled());
        Assert.assertFalse(v3.isEdgeCacheDisabled());
        Assert.assertEquals(4, IteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, IteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertEquals(0, IteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(3, IteratorUtils.count(v2.edges(Direction.IN)));
        Assert.assertEquals(1, IteratorUtils.count(v3.edges(Direction.OUT)));
        Assert.assertEquals(1, IteratorUtils.count(v3.edges(Direction.IN)));

        // Check removals on a cache disabled vertex.
        g.E().hasLabel("v1Out2").drop().iterate();
        g.E().hasLabel("v1Out3").drop().iterate();
        v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        // Ensure caches don't somehow re-enable.
        Assert.assertTrue(v1.isEdgeCacheDisabled());
        Assert.assertFalse(v2.isEdgeCacheDisabled());
        Assert.assertEquals(2, IteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, IteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertEquals(0, IteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(1, IteratorUtils.count(v2.edges(Direction.IN)));
        // v1.
        Assert.assertTrue(g.V().hasLabel("v1").outE("v1Out1").hasNext());
        Assert.assertFalse(g.V().hasLabel("v1").outE("v1Out2").hasNext());
        Assert.assertFalse(g.V().hasLabel("v1").outE("v1Out3").hasNext());
        Assert.assertTrue(g.V().hasLabel("v1").outE("v1Out4").hasNext());
        // v2.
        Assert.assertTrue(g.V().hasLabel("v2").inE("v1Out1").hasNext());
        Assert.assertFalse(g.V().hasLabel("v2").inE("v1Out2").hasNext());
        Assert.assertFalse(g.V().hasLabel("v2").inE("v1Out3").hasNext());
        Assert.assertFalse(g.V().hasLabel("v2").inE("v1Out4").hasNext());
        // v3.
        Assert.assertFalse(g.V().hasLabel("v3").inE("v1Out1").hasNext());
        Assert.assertFalse(g.V().hasLabel("v3").inE("v1Out2").hasNext());
        Assert.assertFalse(g.V().hasLabel("v3").inE("v1Out3").hasNext());
        Assert.assertTrue(g.V().hasLabel("v3").inE("v1Out4").hasNext());
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
