package com.aerospike.firefly.io.cache;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.google.common.collect.Iterators;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.cache.CacheTestsUtils.getCacheDisabledFirefly;
import static com.aerospike.firefly.io.cache.CacheTestsUtils.getCacheWithSizeFirefly;

public class TestEdgeCacheIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        SETUP_GRAPH.close();
    }

    @Test
    public void testCacheEnabled() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
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
        try (final FireflyGraph graph = getCacheDisabledFirefly()) {
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
            Assert.assertTrue(v1.isEdgeCacheOverflowed());
            Assert.assertTrue(v2.isEdgeCacheOverflowed());
        }
    }

    @Test
    public void testCacheLimit() {
        try (final FireflyGraph graph = getCacheWithSizeFirefly(3)) {
            // This time specifically returns referenced instances and not snapshot instantiated ones
            final FireflyTestVertexes vertexes = setupTest(graph, false);
            assertGremlinTraversalAccuracy(graph);

            // The cache will only overflow for threeOutTwoIn, since it has 3 OUT edges which exceeds cache size of 2
            Assert.assertTrue(vertexes.threeOutTwoIn.isEdgeCacheOverflowed());
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.OUT), 3);
            assertEdgeCount(vertexes.threeOutTwoIn.edges(Direction.IN), 2);
            Assert.assertFalse(vertexes.oneOutTwoIn.isEdgeCacheOverflowed());
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.OUT), 1);
            assertEdgeCount(vertexes.oneOutTwoIn.edges(Direction.IN), 2);
            Assert.assertFalse(vertexes.twoOutOneIn.isEdgeCacheOverflowed());
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.OUT), 2);
            assertEdgeCount(vertexes.twoOutOneIn.edges(Direction.IN), 1);
            Assert.assertFalse(vertexes.zeroOutOneIn.isEdgeCacheOverflowed());
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.OUT), 0);
            assertEdgeCount(vertexes.zeroOutOneIn.edges(Direction.IN), 1);
        }
    }

    @Test
    public void testEdgeCacheStorageOnRecord() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            final FireflyIdFactory idFactory = graph.getIdFactory();
            final FireflyVertex v1 = (FireflyVertex) g.addV("v1").next();
            final FireflyVertex v2 = (FireflyVertex) g.addV("v2").next();
            final FireflyEdge e = (FireflyEdge) g.addE("pepperoni").property("toBeRemoved", "RIP" ).from(v1).to(v2).next();
            g.addE("pepperoni").from(v1).to(v2).iterate();
            final FireflyId expectedOutId = idFactory.createCompositeEdgeId((FireflyEdgeId) e.id, v2.id);
            final FireflyId expectedInId = idFactory.createCompositeEdgeId((FireflyEdgeId) e.id, v1.id);

            final Key v1Key = getKey(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, v1.id);
            final Key v2Key = getKey(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, v2.id);
            final Operation getOutEdgeCache = Operation.get(graph.getBaseGraph().OUT_EDGES_BIN);
            final Operation getInEdgeCache = Operation.get(graph.getBaseGraph().IN_EDGES_BIN);
            Record outResult = graph.getBaseGraph().readOperate(null, v1Key, getOutEdgeCache);
            Record inResult = graph.getBaseGraph().readOperate(null, v2Key, getInEdgeCache);

            Map<String, List<Object>> outMap = (Map<String, List<Object>>) outResult.getMap(graph.getBaseGraph().OUT_EDGES_BIN);
            Map<String, List<Object>> inMap = (Map<String, List<Object>>) inResult.getMap(graph.getBaseGraph().IN_EDGES_BIN);
            Assert.assertEquals(1, outMap.size());
            Assert.assertEquals(1, inMap.size());
            Assert.assertEquals(2, outMap.get("pepperoni").size());
            Assert.assertEquals(2, inMap.get("pepperoni").size());
            List<Object> outPepperoniEdges = outMap.get("pepperoni");
            boolean foundExpectedOutEdge = false;
            for (final Object edgeId : outPepperoniEdges) {
                if (edgeId.equals(expectedOutId.getCachedId())) {
                    foundExpectedOutEdge = true;
                }
            }
            Assert.assertTrue("Composite ID found in OUT edge cache", foundExpectedOutEdge);
            List<Object> inPepperoniEdges = inMap.get("pepperoni");
            boolean foundExpectedInEdge = false;
            for (final Object edgeId : inPepperoniEdges) {
                if (edgeId.equals(expectedInId.getCachedId())) {
                    foundExpectedInEdge = true;
                }
            }
            Assert.assertTrue("Composite ID found in IN edge cache", foundExpectedInEdge);

            g.V().hasLabel("v1").outE().has("toBeRemoved").drop().iterate();

            outResult = graph.getBaseGraph().readOperate(null, v1Key, getOutEdgeCache);
            inResult = graph.getBaseGraph().readOperate(null, v2Key, getInEdgeCache);

            outMap = (Map<String, List<Object>>) outResult.getMap(graph.getBaseGraph().OUT_EDGES_BIN);
            inMap = (Map<String, List<Object>>) inResult.getMap(graph.getBaseGraph().IN_EDGES_BIN);
            Assert.assertEquals(1, outMap.size());
            Assert.assertEquals(1, inMap.size());
            Assert.assertEquals(1, outMap.get("pepperoni").size());
            Assert.assertEquals(1, inMap.get("pepperoni").size());
            outPepperoniEdges = outMap.get("pepperoni");
            for (final Object edgeId : outPepperoniEdges) {
                if (edgeId.equals(expectedOutId.getCachedId())) {
                    Assert.fail("Composite ID was not removed from OUT edge cache");
                }
            }
            inPepperoniEdges = inMap.get("pepperoni");
            for (final Object edgeId : inPepperoniEdges) {
                if (edgeId.equals(expectedInId.getCachedId())) {
                    Assert.fail("Composite ID was not removed from IN edge cache");
                }
            }
        }
    }

    @Test
    public void testAddAndRemoveEdges() {
        try (final FireflyGraph graph = getCacheWithSizeFirefly(4)) {
            testAddAndRemoveEdges(graph);
        }
    }

    @Test
    public void testRemoveVertexRemovesEdgeFromAdjacentVertices() {
        try (final FireflyGraph graph = getCacheWithSizeFirefly(3)) {
            graph.getBaseGraph().dropDatabase(graph, false);
            final GraphTraversalSource g = graph.traversal();

            // No supernodes
            Vertex v1 = g.addV("v1").next();
            Vertex v2 = g.addV("v2").next();
            Vertex v3 = g.addV("v3").next();
            g.addE("1to2").from(v1).to(v2).iterate();
            g.addE("1to2").from(v1).to(v2).iterate();
            g.addE("2to3").from(v2).to(v3).iterate();
            g.addE("2to3").from(v2).to(v3).iterate();
            g.V(v2.id()).drop().iterate();
            Assert.assertFalse(g.V(v2.id()).hasNext());
            FireflyVertex fv1 = (FireflyVertex) g.V(v1.id()).next();
            FireflyVertex fv3 = (FireflyVertex) g.V(v3.id()).next();
            Assert.assertFalse(fv1.getEdgeIdsFromVertex(Direction.BOTH, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertFalse(fv3.getEdgeIdsFromVertex(Direction.BOTH, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertFalse(g.E().hasNext());
            g.V().drop().iterate();

            // Remove supernode (v2)
            v1 = g.addV("v1").next();
            v2 = g.addV("v2").next();
            v3 = g.addV("v3").next();
            Vertex v4 = g.addV("v4").next();
            g.addE("1to2").from(v1).to(v2).iterate();
            g.addE("1to2").from(v1).to(v2).iterate();
            g.addE("2to3").from(v2).to(v3).iterate();
            g.addE("2to3").from(v2).to(v3).iterate();
            g.addE("4to2").from(v4).to(v2).iterate();
            g.addE("4to2").from(v4).to(v2).iterate();
            FireflyVertex fv2 = (FireflyVertex) g.V(v2.id()).next();
            Assert.assertTrue(fv2.isEdgeCacheOverflowed());
            g.V(v2.id()).drop().iterate();
            fv1 = (FireflyVertex) g.V(v1.id()).next();
            fv3 = (FireflyVertex) g.V(v3.id()).next();
            FireflyVertex fv4 = (FireflyVertex) g.V(v4.id()).next();
            Assert.assertFalse(fv1.getEdgeIdsFromVertex(Direction.BOTH, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertFalse(fv3.getEdgeIdsFromVertex(Direction.BOTH, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertFalse(fv4.getEdgeIdsFromVertex(Direction.BOTH, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertFalse(g.E().hasNext());
            g.V().drop().iterate();

            // Remove attached to a supernode (v3)
            v1 = g.addV("v1").next();
            v2 = g.addV("v2").next();
            v3 = g.addV("v3").next();
            v4 = g.addV("v4").next();
            Vertex v5 = g.addV("v5").next();
            g.addE("1to2").from(v1).to(v2).iterate();
            g.addE("1to2").from(v1).to(v2).iterate();
            g.addE("2to3").from(v2).to(v3).iterate();
            g.addE("2to3").from(v2).to(v3).iterate();
            g.addE("3to4").from(v3).to(v4).iterate();
            g.addE("3to5").from(v3).to(v5).iterate();
            g.addE("3to4").from(v3).to(v4).iterate();
            g.addE("3to5").from(v3).to(v5).iterate();
            fv2 = (FireflyVertex) g.V(v2.id()).next();
            Assert.assertFalse(fv2.isEdgeCacheOverflowed());
            fv3 = (FireflyVertex) g.V(v3.id()).next();
            Assert.assertTrue(fv3.isEdgeCacheOverflowed());
            g.V(v2.id()).drop().iterate();
            fv1 = (FireflyVertex) g.V(v1.id()).next();
            Assert.assertFalse(fv1.getEdgeIdsFromVertex(Direction.BOTH, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            fv3 = (FireflyVertex) g.V(v3.id()).next();
            Assert.assertFalse(fv3.getEdgeIdsFromVertex(Direction.IN, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertEquals(4, Iterators.size(fv3.getEdgeIdsFromVertex(Direction.OUT, Collections.EMPTY_SET, Collections.emptyList())));
            g.V(v4.id()).drop().iterate();
            fv3 = (FireflyVertex) g.V(v3.id()).next();
            Assert.assertEquals(2, Iterators.size(fv3.getEdgeIdsFromVertex(Direction.OUT, Collections.EMPTY_SET, Collections.emptyList())));
            g.V(v5.id()).drop().iterate();
            fv3 = (FireflyVertex) g.V(v3.id()).next();
            Assert.assertFalse(fv3.getEdgeIdsFromVertex(Direction.OUT, Collections.EMPTY_SET, Collections.emptyList()).hasNext());
            Assert.assertFalse(g.E().hasNext());
        }
    }

    private void testAddAndRemoveEdges(final FireflyGraph graph) {
        graph.getBaseGraph().dropDatabase(graph, false);

        // This tests adding and removal of edges, in states where the cache overflow is enabled and disabled, as well
        // as the cache state being triggered to change.
        final GraphTraversalSource g = graph.traversal();
        FireflyVertex v1 = (FireflyVertex) g.addV("v1").next();
        FireflyVertex v2 = (FireflyVertex) g.addV("v2").next();
        FireflyVertex v3 = (FireflyVertex) g.addV("v3").next();
        Assert.assertFalse(v1.isEdgeCacheOverflowed());
        Assert.assertFalse(v2.isEdgeCacheOverflowed());
        Assert.assertFalse(v3.isEdgeCacheOverflowed());

        // 3 OUT shouldn't disable cache.
        g.addE("v1Out1").from(v1).to(v2).next();
        g.addE("v1Out2").from(v1).to(v2).next();
        g.addE("v1Out3").from(v1).to(v2).next();
        Assert.assertFalse(v1.isEdgeCacheOverflowed());
        Assert.assertFalse(v2.isEdgeCacheOverflowed());
        Assert.assertEquals(3, FireflyCloseableIteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(0, FireflyCloseableIteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertEquals(0, FireflyCloseableIteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(3, FireflyCloseableIteratorUtils.count(v2.edges(Direction.IN)));

        // Ensure OUT and IN counts (4 total) are separate for triggering cache disable.
        g.addE("v1In1").from(v3).to(v1).next();
        Assert.assertEquals(3, FireflyCloseableIteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertFalse(v1.isEdgeCacheOverflowed());
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v3.edges(Direction.OUT)));
        Assert.assertEquals(0, FireflyCloseableIteratorUtils.count(v3.edges(Direction.IN)));
        Assert.assertFalse(v3.isEdgeCacheOverflowed());

        // Check edge removal when cache is enabled still.
        Assert.assertTrue(g.V().hasLabel("v1").outE("v1Out3").hasNext());
        Assert.assertTrue(g.V().hasLabel("v2").inE("v1Out3").hasNext());
        g.E().hasLabel("v1Out3").drop().iterate();
        // Need to grab the vertexes again to refresh its state in the JVM cache
        v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(2, FireflyCloseableIteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertFalse(v1.isEdgeCacheOverflowed());
        Assert.assertEquals(0, FireflyCloseableIteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(2, FireflyCloseableIteratorUtils.count(v2.edges(Direction.IN)));
        Assert.assertFalse(v2.isEdgeCacheOverflowed());
        Assert.assertFalse(g.V().hasLabel("v1").outE("v1Out3").hasNext());
        Assert.assertFalse(g.V().hasLabel("v2").inE("v1Out3").hasNext());

        // Disable the cache for v1.
        g.addE("v1Out3").from(v1).to(v2).next();
        // Add to v3 here to ensure cache counter logic is properly decoupled between both ends of the edge.
        g.addE("v1Out4").from(v1).to(v3).next();
        Assert.assertTrue(v1.isEdgeCacheOverflowed());
        // This also implicitly ensures the drop of the edge on v2 earlier properly decremented its edge count.
        Assert.assertFalse(v2.isEdgeCacheOverflowed());
        Assert.assertFalse(v3.isEdgeCacheOverflowed());
        // Sanity check on generated vertices on reading from database.
        v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        v3 = (FireflyVertex) g.V().hasLabel("v3").next();
        Assert.assertTrue(v1.isEdgeCacheOverflowed());
        Assert.assertFalse(v2.isEdgeCacheOverflowed());
        Assert.assertFalse(v3.isEdgeCacheOverflowed());
        Assert.assertEquals(4, FireflyCloseableIteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertEquals(0, FireflyCloseableIteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(3, FireflyCloseableIteratorUtils.count(v2.edges(Direction.IN)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v3.edges(Direction.OUT)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v3.edges(Direction.IN)));

        // Check removals on a cache disabled vertex.
        g.E().hasLabel("v1Out2").drop().iterate();
        g.E().hasLabel("v1Out3").drop().iterate();
        v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        // Ensure caches don't somehow re-enable.
        Assert.assertTrue(v1.isEdgeCacheOverflowed());
        Assert.assertFalse(v2.isEdgeCacheOverflowed());
        Assert.assertEquals(2, FireflyCloseableIteratorUtils.count(v1.edges(Direction.OUT)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v1.edges(Direction.IN)));
        Assert.assertEquals(0, FireflyCloseableIteratorUtils.count(v2.edges(Direction.OUT)));
        Assert.assertEquals(1, FireflyCloseableIteratorUtils.count(v2.edges(Direction.IN)));
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
        graph.getBaseGraph().dropDatabase(graph, false);
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
        Assert.assertEquals(expected, amount);
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
