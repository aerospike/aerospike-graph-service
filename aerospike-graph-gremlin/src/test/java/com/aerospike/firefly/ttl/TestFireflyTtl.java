package com.aerospike.firefly.ttl;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES_SINDEX;

public class TestFireflyTtl {
    private static final Configuration DEFAULT_TTL_CONFIG = getConfig(2);
    private static final Configuration DEFAULT_CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private FireflyGraph graph;
    private FireflyGraph noTtlGraph; // This is required to ensure TTL is working outside of lazy evaluation

    @Before
    public void beforeEach() {
        this.graph = FireflyGraph.open(DEFAULT_TTL_CONFIG);
        this.graph.getBaseGraph().dropDatabase(graph, false);
        this.noTtlGraph = FireflyGraph.open(DEFAULT_CONFIG);
    }

    @After
    public void afterEach() {
        this.graph.getBaseGraph().dropDatabase(graph, true);
        this.graph.close();
        this.noTtlGraph.close();
    }

    @Test
    public void testTtl() throws InterruptedException {
        assertTtlAccuracy(this.graph, this.noTtlGraph);
    }

    @Test
    public void testTtlVertexAlreadyDeleted() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        final GraphTraversalSource g2 = this.noTtlGraph.traversal();
        // Put this Vertex expiry in the 2-4 second scan and schedule its deletion, and then delete it first manually
        // to ensure it doesn't break the TTL scheduler.
        final Vertex v1 = g.addV("v1").property("~ttl", 3).next();
        final Vertex v2 = g.addV("v2").property("~ttl", 3).next();
        Assert.assertTrue(g2.V(v1.id()).hasNext());
        Assert.assertTrue(g2.V(v2.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertTrue(g2.V(v1.id()).hasNext());
        Assert.assertTrue(g2.V(v2.id()).hasNext());
        g2.V(v1.id()).drop().iterate();
        Assert.assertFalse(g2.V(v1.id()).hasNext());
        Assert.assertTrue(g2.V(v2.id()).hasNext());
        Thread.sleep(2000);
        Assert.assertFalse(g2.V(v2.id()).hasNext());
    }

    @Test
    public void testTtlEdgeAlreadyDeletedSingleEdge() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        final GraphTraversalSource g2 = this.noTtlGraph.traversal();
        // Put this Edge expiry in the 2-4 second scan and schedule its deletion, and then delete it first manually
        // to ensure it doesn't break the TTL scheduler. This is for when the Edge is the only one in the Edge pack.
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        Edge e10 = null;
        for (int i = 0; i < 10; i++) {
            if (i == 9) {
                e10 = g.addE("edge").property("~ttl", 3).from(v1).to(v2).next();
            } else {
                g.addE("edge").from(v1).to(v2).iterate();
            }
        }
        final Edge e11 = g.addE("edge").property("~ttl", 3).from(v1).to(v2).next();
        Assert.assertTrue(g2.E(e10.id()).hasNext());
        Assert.assertTrue(g2.E(e11.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertTrue(g2.E(e10.id()).hasNext());
        Assert.assertTrue(g2.E(e11.id()).hasNext());
        g2.E(e11.id()).drop().iterate();
        Assert.assertFalse(g2.E(e11.id()).hasNext());
        Assert.assertTrue(g2.E(e10.id()).hasNext());
        Thread.sleep(2000);
        Assert.assertFalse(g2.E(e10.id()).hasNext());
    }

    @Test
    public void testTtlEdgeAlreadyDeletedMultipleEdge() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        final GraphTraversalSource g2 = this.noTtlGraph.traversal();
        // Put this Edge expiry in the 2-4 second scan and schedule its deletion, and then delete it first manually
        // to ensure it doesn't break the TTL scheduler. This is for when the Edge is not the only one in the Edge pack.
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        final Edge e1 = g.addE("edge").property("~ttl", 3).from(v1).to(v2).next();
        final Edge e2 = g.addE("edge").property("~ttl", 3).from(v1).to(v2).next();
        Assert.assertTrue(g2.E(e1.id()).hasNext());
        Assert.assertTrue(g2.E(e2.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertTrue(g2.E(e1.id()).hasNext());
        Assert.assertTrue(g2.E(e2.id()).hasNext());
        g2.E(e1.id()).drop().iterate();
        Assert.assertFalse(g2.E(e1.id()).hasNext());
        Assert.assertTrue(g2.E(e2.id()).hasNext());
        Thread.sleep(2000);
        Assert.assertFalse(g2.E(e2.id()).hasNext());
    }

    @Test
    public void testInvalidTtlValue() {
        final GraphTraversalSource g = this.graph.traversal();
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        try {
            g.addV("vertex").property("~ttl", "string").iterate();
            Assert.fail("String ttl value succeeded for adding Vertex");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        try {
            g.V(v1.id()).property("~ttl", "string").iterate();
            Assert.fail("String ttl value succeeded for adding property to Vertex");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        try {
            g.addE("edge").property("~ttl", "string").from(v1).to(v2).iterate();
            Assert.fail("String ttl value succeeded for adding Edge");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        try {
            final Edge e = g.addE("edge").from(v1).to(v2).next();
            g.E(e.id()).property("~ttl", "string").iterate();
            Assert.fail("String ttl value succeeded for adding property to Edge");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testUpdatingTtlVertex() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        final GraphTraversalSource g2 = this.graph.traversal();

        // Increase TTL
        Vertex v = g.addV("v").property("~ttl", 4).next();
        Thread.sleep(2500);
        g.V(v.id()).property("~ttl", 5).iterate();
        Thread.sleep(4000);
        // If TTL update of the original 4 seconds didn't work, then the element is gone now because it has been 6.5 seconds
        Assert.assertTrue(g2.V(v.id()).hasNext());
        Thread.sleep(3500);
        // The new TTL is 2.5 seconds + 5 seconds so now that it has been 10 seconds it should be gone
        Assert.assertFalse(g.V(v.id()).hasNext());

        // Decrease TTL
        v = g.addV("v").property("~ttl", 10).next();
        g.V(v.id()).property("~ttl", 3).next();
        Assert.assertTrue(g2.V(v.id()).hasNext());
        Thread.sleep(5500);
        Assert.assertFalse(g2.V(v.id()).hasNext());
    }

    @Test
    public void testUpdatingTtlEdge() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        final GraphTraversalSource g2 = this.noTtlGraph.traversal();
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();

        // Increase TTL
        Edge e = g.addE("e").property("~ttl", 4).from(v1).to(v2).next();
        Thread.sleep(2500);
        g.E(e.id()).property("~ttl", 5).iterate();
        Thread.sleep(4000);
        // If TTL update of the original 4 seconds didn't work, then the element is gone now because it has been 6.5 seconds
        Assert.assertTrue(g2.E(e.id()).hasNext());
        Thread.sleep(3500);
        // The new TTL is 2.5 seconds + 5 seconds so now that it has been 10 seconds it should be gone
        Assert.assertFalse(g2.E(e.id()).hasNext());

        // Decrease TTL
        e = g.addE("v").property("~ttl", 10).from(v1).to(v2).next();
        g.E(e.id()).property("~ttl", 3).next();
        Assert.assertTrue(g2.E(e.id()).hasNext());
        Thread.sleep(5500);
        Assert.assertFalse(g2.E(e.id()).hasNext());
    }

    @Test
    public void testRemoveEdgeWithNoTtl() {
        // This test is just to ensure that the Edge removal operations don't crash if they operate on a non-existing bin
        GraphTraversalSource g = this.graph.traversal();
        Vertex v1 = g.addV("v1").next();
        Vertex v2 = g.addV("v2").next();
        g.addE("e1").from(v1).to(v2).iterate();
        g.addE("e2").from(v1).to(v2).iterate();
        g.E().drop().iterate();
    }

    @Test
    public void testCleanupTtl() throws InterruptedException {
        GraphTraversalSource g = this.graph.traversal();
        GraphTraversalSource g2 = this.noTtlGraph.traversal();
        Vertex v1 = g.addV("v1").next();
        Vertex v2 = g.addV("v2").next();
        g.addE("e1").from(v1).to(v2).iterate();
        g.addE("e2").from(v1).to(v2).iterate();
        g.addV("v3").next();
        Assert.assertTrue(g2.V().hasLabel("v3").hasNext());
        Assert.assertTrue(g2.E().hasLabel("e2").hasNext());
        Thread.sleep(4500);

        // Doesn't really make sense to enter a negative TTL in practice, but allows us to put elements that exist
        // outside the intervals that we record as purged, meaning these elements are only purged by the cleanup cycle.
        g.V().hasLabel("v3").property("~ttl", -1000).iterate();
        g.E().hasLabel("e2").property("~ttl", -1000).iterate();
        Thread.sleep(2500);
        Assert.assertFalse(g2.V().hasLabel("v3").hasNext());
        Assert.assertFalse(g2.E().hasLabel("e2").hasNext());
    }

    @Test
    public void testLazyEvaluationVerticesScan() throws InterruptedException {
        this.graph.close();
        this.graph = FireflyGraph.open(getConfig(1000));
        GraphTraversalSource g = this.graph.traversal();
        final Vertex v = g.addV("v1").property("~ttl", 1).next();
        g.addV("v2").iterate();
        Assert.assertTrue(g.V().hasLabel("v1").hasNext());
        Thread.sleep(2000);
        Assert.assertFalse(g.V().hasLabel("v1").hasNext());
        Assert.assertFalse(g.V(v.id()).hasNext());
        Assert.assertTrue(g.V().hasLabel("v2").hasNext());
        try {
            g.V().hasLabel("v1").next();
            Assert.fail("Traversal should've thrown an exception");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof NoSuchElementException);
        }
        try {
            g.V(v.id()).next();
            Assert.fail("Traversal should've thrown an exception");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof NoSuchElementException);
        }
    }

    @Test
    public void testLazyEvaluationVerticesSindex() throws InterruptedException {
        this.graph.close();
        this.graph = FireflyGraph.open(getConfigSindex(1000));
        GraphTraversalSource g = this.graph.traversal();
        final Vertex v = g.addV("v1").property("~ttl", 1).next();
        g.addV("v2").iterate();
        Assert.assertTrue(g.V().hasLabel("v1").hasNext());
        Thread.sleep(2000);
        Assert.assertFalse(g.V().hasLabel("v1").hasNext());
        Assert.assertFalse(g.V(v.id()).hasNext());
        Assert.assertTrue(g.V().hasLabel("v2").hasNext());
        try {
            g.V().hasLabel("v1").next();
            Assert.fail("Traversal should've thrown an exception");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof NoSuchElementException);
        }
        try {
            g.V(v.id()).next();
            Assert.fail("Traversal should've thrown an exception");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof NoSuchElementException);
        }
    }

    @Test
    public void testLazyEvaluationVertexComposite() throws InterruptedException {
        this.graph.close();
        this.graph = FireflyGraph.open(getConfig(1000));
        GraphTraversalSource g = this.graph.traversal();
        final Vertex v1 = g.addV("v1").property("~ttl", 1).next();
        final Vertex v2 = g.addV("v2").next();
        g.addE("e").from(v2).to(v1).iterate();
        Assert.assertTrue(g.V().hasLabel("v1").hasNext());
        Assert.assertTrue(g.V(v2.id()).out().hasNext());
        Thread.sleep(2000);
        Assert.assertFalse(g.V().hasLabel("v1").hasNext());
        Assert.assertFalse(g.V(v2.id()).out().hasNext());
    }

    @Test
    public void testLazyEvaluationFromEdge() throws InterruptedException {
        final Configuration config = getConfig(1000);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY, "false");

        this.graph.close();
        this.graph = FireflyGraph.open(config);
        GraphTraversalSource g = this.graph.traversal();
        final Vertex v1 = g.addV("v1").property("~ttl", 1).next();
        final Vertex v2 = g.addV("v2").next();
        final Edge e = g.addE("e").from(v2).to(v1).next();
        Assert.assertEquals(v1.id(), g.E(e.id()).inV().next().id());
        Thread.sleep(2000);
        // TODO GRAPH-1145: Currently this just tests that we don't cause null exceptions. Change this when lazy
        //                  evaluation works for a Vertex directly grabbed from an Edge.
        Assert.assertTrue(g.E(e.id()).inV().hasNext());
        final Vertex v1FromEdge = g.E(e.id()).inV().next();
        Assert.assertNotNull(v1FromEdge);
        Assert.assertEquals(v1.id(), v1FromEdge.id());
        Assert.assertFalse(g.V(v1FromEdge.id()).hasNext());
    }

    private static void assertTtlAccuracy(final FireflyGraph graph, final FireflyGraph noTtlGraph) throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        final GraphTraversalSource g2 = noTtlGraph.traversal();
        final Vertex v1 = g.addV("v1").property("~ttl", 3).next();
        final Vertex v2 = g.addV("v2").next();
        final Vertex v3 = g.addV("v3").next();
        final Vertex v4 = g.addV("v4").next();
        final Vertex v5 = g.addV("v5").property("~ttl", 300).next();
        final Edge v1tov2 = g.addE("edge").from(v1).to(v2).next();
        final Edge v2tov1 = g.addE("edge").from(v2).to(v1).next();
        final Edge v2tov3 = g.addE("edge").property("~ttl", 3).from(v2).to(v3).next();
        final Edge v3tov2 = g.addE("edge").from(v3).to(v2).next();
        final Edge v2tov3longTtl = g.addE("edge").property("~ttl", 300).from(v2).to(v3).next();
        g.V(v4.id()).property("~ttl", 2).iterate();

        // Assert proper baseline
        Assert.assertTrue(g2.V(v1.id()).hasNext());
        Assert.assertTrue(g2.V(v2.id()).hasNext());
        Assert.assertTrue(g2.V(v3.id()).hasNext());
        Assert.assertTrue(g2.V(v4.id()).hasNext());
        Assert.assertTrue(g2.V(v5.id()).hasNext());
        Assert.assertTrue(g2.E(v1tov2.id()).hasNext());
        Assert.assertTrue(g2.E(v2tov1.id()).hasNext());
        Assert.assertTrue(g2.E(v2tov3.id()).hasNext());
        Assert.assertTrue(g2.E(v3tov2.id()).hasNext());
        Assert.assertTrue(g2.E(v2tov3longTtl.id()).hasNext());
        Assert.assertFalse(g.V().has("~ttl").hasNext());
        Assert.assertFalse(g.E().has("~ttl").hasNext());

        // Sleep for 5.5s for TTL to kick in (3s TTL + 2s interval + .5s for test stability)
        Thread.sleep(5500);
        // Assert expected TTL elements are removed and edges attached to TTL vertices too as well
        Assert.assertFalse(g2.V(v1.id()).hasNext());
        Assert.assertTrue(g2.V(v2.id()).hasNext());
        Assert.assertTrue(g2.V(v3.id()).hasNext());
        Assert.assertFalse(g2.V(v4.id()).hasNext());
        Assert.assertTrue(g2.V(v5.id()).hasNext());
        Assert.assertFalse(g2.E(v1tov2.id()).hasNext());
        Assert.assertFalse(g2.E(v2tov1.id()).hasNext());
        Assert.assertFalse(g2.E(v2tov3.id()).hasNext());
        Assert.assertTrue(g2.E(v3tov2.id()).hasNext());
        Assert.assertTrue(g2.E(v2tov3longTtl.id()).hasNext());

        g.E(v3tov2.id()).property("~ttl", 1).iterate();
        Thread.sleep(3500);
        Assert.assertFalse(g2.E(v3tov2.id()).hasNext());
    }

    private static Configuration getConfig(final int purgeIntervalSeconds) {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.TTL_ENABLED_FLAG.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.TTL_PURGE_INTERVAL_SECONDS.toLowerCase(), String.valueOf(purgeIntervalSeconds));
        return config;
    }

    private static Configuration getConfigSindex(final int purgeIntervalSeconds) {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES_SINDEX);
        config.setProperty(ConfigurationHelper.Keys.TTL_ENABLED_FLAG.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.TTL_PURGE_INTERVAL_SECONDS.toLowerCase(), String.valueOf(purgeIntervalSeconds));
        return config;
    }
}
