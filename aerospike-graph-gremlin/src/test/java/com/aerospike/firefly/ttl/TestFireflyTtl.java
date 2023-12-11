package com.aerospike.firefly.ttl;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestFireflyTtl {
    private static final Configuration DEFAULT_TTL_CONFIG = getConfig(2, false);
    private FireflyGraph graph;

    @Before
    public void beforeEach() {
        this.graph = FireflyGraph.open(DEFAULT_TTL_CONFIG);
        this.graph.getBaseGraph().dropDatabase(graph, false);
    }

    @After
    public void afterEach() {
        this.graph.getBaseGraph().dropDatabase(graph, false);
        this.graph.close();
    }

    @Test
    public void testTtl() throws InterruptedException {
        assertTtlAccuracy(this.graph);
    }

    @Test
    public void testTtlScheduledImmediately() throws InterruptedException {
        this.graph.close();
        final Configuration longIntervalConfig = getConfig(10, false);
        this.graph = FireflyGraph.open(longIntervalConfig);
        assertTtlAccuracy(this.graph);
    }

    @Test
    public void testTtlVertexAlreadyDeleted() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        // Put this Vertex expiry in the 2-4 second scan and schedule its deletion, and then delete it first manually
        // to ensure it doesn't break the TTL scheduler.
        final Vertex v1 = g.addV("v1").property("~ttl", 3).next();
        final Vertex v2 = g.addV("v2").property("~ttl", 3).next();
        Assert.assertTrue(g.V(v1.id()).hasNext());
        Assert.assertTrue(g.V(v2.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertTrue(g.V(v1.id()).hasNext());
        Assert.assertTrue(g.V(v2.id()).hasNext());
        g.V(v1.id()).drop().iterate();
        Assert.assertFalse(g.V(v1.id()).hasNext());
        Assert.assertTrue(g.V(v2.id()).hasNext());
        Thread.sleep(1000);
        Assert.assertFalse(g.V(v2.id()).hasNext());
    }

    @Test
    public void testTtlEdgeAlreadyDeletedSingleEdge() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
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
        Assert.assertTrue(g.E(e10.id()).hasNext());
        Assert.assertTrue(g.E(e11.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertTrue(g.E(e10.id()).hasNext());
        Assert.assertTrue(g.E(e11.id()).hasNext());
        g.E(e11.id()).drop().iterate();
        Assert.assertFalse(g.E(e11.id()).hasNext());
        Assert.assertTrue(g.E(e10.id()).hasNext());
        Thread.sleep(1000);
        Assert.assertFalse(g.E(e10.id()).hasNext());
    }

    @Test
    public void testTtlEdgeAlreadyDeletedMultipleEdge() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        // Put this Edge expiry in the 2-4 second scan and schedule its deletion, and then delete it first manually
        // to ensure it doesn't break the TTL scheduler. This is for when the Edge is not the only one in the Edge pack.
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        final Edge e1 = g.addE("edge").property("~ttl", 3).from(v1).to(v2).next();
        final Edge e2 = g.addE("edge").property("~ttl", 3).from(v1).to(v2).next();
        Assert.assertTrue(g.E(e1.id()).hasNext());
        Assert.assertTrue(g.E(e2.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertTrue(g.E(e1.id()).hasNext());
        Assert.assertTrue(g.E(e2.id()).hasNext());
        g.E(e1.id()).drop().iterate();
        Assert.assertFalse(g.E(e1.id()).hasNext());
        Assert.assertTrue(g.E(e2.id()).hasNext());
        Thread.sleep(1000);
        Assert.assertFalse(g.E(e2.id()).hasNext());
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

        // Increase TTL
        Vertex v = g.addV("v").property("~ttl", 4).next();
        Thread.sleep(1000);
        g.V(v.id()).property("~ttl", 4).iterate();
        Thread.sleep(3500);
        // If TTL update of the original 4 seconds didn't work, then the element is gone now because it has been 4.5 seconds
        Assert.assertTrue(g.V(v.id()).hasNext());
        Thread.sleep(1000);
        // The new TTL is 1 second + 4 seconds so now that it has been 5.5 seconds it should be gone
        Assert.assertFalse(g.V(v.id()).hasNext());

        // Decrease TTL
        v = g.addV("v").property("~ttl", 4).next();
        g.V(v.id()).property("~ttl", 2).next();
        Assert.assertTrue(g.V(v.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertFalse(g.V(v.id()).hasNext());
    }

    @Test
    public void testUpdatingTtlEdge() throws InterruptedException {
        final GraphTraversalSource g = this.graph.traversal();
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();

        // Increase TTL
        Edge e = g.addE("e").property("~ttl", 4).from(v1).to(v2).next();
        Thread.sleep(1000);
        g.E(e.id()).property("~ttl", 4).iterate();
        Thread.sleep(3500);
        // If TTL update of the original 4 seconds didn't work, then the element is gone now because it has been 4.5 seconds
        Assert.assertTrue(g.E(e.id()).hasNext());
        Thread.sleep(1000);
        // The new TTL is 1000ms + 4000ms so now that it has been 5.5 seconds it should be gone
        Assert.assertFalse(g.E(e.id()).hasNext());

        // Decrease TTL
        e = g.addE("v").property("~ttl", 4).from(v1).to(v2).next();
        g.E(e.id()).property("~ttl", 2).next();
        Assert.assertTrue(g.E(e.id()).hasNext());
        Thread.sleep(2500);
        Assert.assertFalse(g.E(e.id()).hasNext());
    }

    @Test
    public void testTtlUpdateAnytimeFalseVertices() throws InterruptedException {
        // TTL values that are lower than the TTL_PURGE_INTERVAL can not be increased.
        GraphTraversalSource g = this.graph.traversal();
        Vertex v = g.addV("v").property("~ttl", 1).next();
        g.V(v.id()).property("~ttl", 0).iterate();
        // See that decreasing TTL works.
        Thread.sleep(500);
        Assert.assertFalse(g.V(v.id()).hasNext());
        // See that increasing TTL doesn't work.
        v = g.addV("v").property("~ttl", 1).next();
        g.V(v.id()).property("~ttl", 3).iterate();
        Thread.sleep(2000);
        Assert.assertFalse(g.V(v.id()).hasNext());

        // Elements with an expiry time that is within the current time + TTL_PURGE_INTERVAL can not increase its TTL.
        // Reset the graph to start.
        this.afterEach();
        final Configuration config = getConfig(3, false);
        this.graph = FireflyGraph.open(config);
        g = this.graph.traversal();
        v = g.addV("v").property("~ttl", 5).next();
        Thread.sleep(3500);
        // See that decreasing TTL works.
        g.V(v.id()).property("~ttl", 1).iterate();
        Thread.sleep(1500);
        Assert.assertFalse(g.V(v.id()).hasNext());
        // See that increasing TTL doesn't work.
        v = g.addV("v").property("~ttl", 3).next(); // This should expire at 8 seconds, caught by the 6-9 second interval.
        Thread.sleep(2000);
        g.V(v.id()).property("~ttl", 10).iterate();
        // Check at the 8.5 second mark.
        Thread.sleep(1500);
        Assert.assertFalse(g.V(v.id()).hasNext());
    }

    @Test
    public void testTtlUpdateAnytimeFalseEdges() throws InterruptedException {
        // TTL values that are lower than the TTL_PURGE_INTERVAL can not be increased.
        GraphTraversalSource g = this.graph.traversal();
        Vertex v1 = g.addV("v1").next();
        Vertex v2 = g.addV("v2").next();
        Edge e = g.addE("e").property("~ttl", 1).from(v1).to(v2).next();
        g.E(e.id()).property("~ttl", 0).iterate();
        // See that decreasing TTL works.
        Thread.sleep(500);
        Assert.assertFalse(g.E(e.id()).hasNext());
        // See that increasing TTL doesn't work.
        e = g.addE("e").property("~ttl", 1).from(v1).to(v2).next();
        g.E(e.id()).property("~ttl", 3).iterate();
        Thread.sleep(2000);
        Assert.assertFalse(g.E(e.id()).hasNext());

        // Elements with an expiry time that is within the current time + TTL_PURGE_INTERVAL can not increase its TTL.
        // Reset the graph to start.
        this.afterEach();
        final Configuration config = getConfig(3, false);
        this.graph = FireflyGraph.open(config);
        g = this.graph.traversal();
        v1 = g.addV("v1").next();
        v2 = g.addV("v2").next();
        e = g.addE("e").property("~ttl", 5).from(v1).to(v2).next();
        Thread.sleep(3500);
        // See that decreasing TTL works.
        g.E(e.id()).property("~ttl", 1).iterate();
        Thread.sleep(1500);
        Assert.assertFalse(g.E(e.id()).hasNext());
        // See that increasing TTL doesn't work.
        e = g.addE("e").property("~ttl", 3).from(v1).to(v2).next(); // This should expire at 8 seconds, caught by the 6-9 second interval.
        Thread.sleep(2000);
        g.E(e.id()).property("~ttl", 10).iterate();
        // Check at the 8.5 second mark.
        Thread.sleep(1500);
        Assert.assertFalse(g.E(e.id()).hasNext());
    }

    @Test
    public void testTtlUpdateAnytimeTrueVertices() throws InterruptedException {
        this.afterEach();
        Configuration config = getConfig(2, true);
        this.graph = FireflyGraph.open(config);
        // Test updating TTL values that are lower than the TTL_PURGE_INTERVAL.
        GraphTraversalSource g = this.graph.traversal();
        Vertex v = g.addV("v").property("~ttl", 1).next();
        g.V(v.id()).property("~ttl", 0).iterate();
        // See that decreasing TTL works.
        Thread.sleep(500);
        Assert.assertFalse(g.V(v.id()).hasNext());
        // See that increasing TTL works.
        v = g.addV("v").property("~ttl", 1).next();
        g.V(v.id()).property("~ttl", 3).iterate();
        Thread.sleep(2000);
        Assert.assertTrue(g.V(v.id()).hasNext());
        Thread.sleep(1500);
        Assert.assertFalse(g.V(v.id()).hasNext());

        // Test Elements with an expiry time that is within the current time + TTL_PURGE_INTERVAL.
        // Reset the graph to start.
        this.afterEach();
        config = getConfig(3, true);
        this.graph = FireflyGraph.open(config);
        g = this.graph.traversal();
        v = g.addV("v").property("~ttl", 5).next();
        Thread.sleep(3500);
        // See that decreasing TTL works.
        g.V(v.id()).property("~ttl", 1).iterate();
        Thread.sleep(1500);
        Assert.assertFalse(g.V(v.id()).hasNext());
        // See that increasing TTL works.
        v = g.addV("v").property("~ttl", 3).next(); // This should expire at 8 seconds, caught by the 6-9 second interval.
        Thread.sleep(2000);
        // Update TTL to be at the 9 second mark.
        g.V(v.id()).property("~ttl", 2).iterate();
        // Check at the 8.5 second mark.
        Thread.sleep(1500);
        Assert.assertTrue(g.V(v.id()).hasNext());
        // Check at the 9.5 second mark.
        Thread.sleep(1000);
        Assert.assertFalse(g.V(v.id()).hasNext());
    }

    @Test
    public void testTtlUpdateAnytimeTrueEdges() throws InterruptedException {
        this.afterEach();
        Configuration config = getConfig(2, true);
        this.graph = FireflyGraph.open(config);
        // Test updating TTL values that are lower than the TTL_PURGE_INTERVAL.
        GraphTraversalSource g = this.graph.traversal();
        Vertex v1 = g.addV("v1").next();
        Vertex v2 = g.addV("v2").next();
        Edge e = g.addE("e").property("~ttl", 1).from(v1).to(v2).next();
        g.E(e.id()).property("~ttl", 0).iterate();
        // See that decreasing TTL works.
        Thread.sleep(500);
        Assert.assertFalse(g.E(e.id()).hasNext());
        // See that increasing TTL works.
        e = g.addE("e").property("~ttl", 1).from(v1).to(v2).next();
        g.E(e.id()).property("~ttl", 3).iterate();
        Thread.sleep(2000);
        Assert.assertTrue(g.E(e.id()).hasNext());
        Thread.sleep(1500);
        Assert.assertFalse(g.E(e.id()).hasNext());

        // Test Elements with an expiry time that is within the current time + TTL_PURGE_INTERVAL.
        // Reset the graph to start.
        this.afterEach();
        config = getConfig(3, true);
        this.graph = FireflyGraph.open(config);
        g = this.graph.traversal();
        v1 = g.addV("v1").next();
        v2 = g.addV("v2").next();
        e = g.addE("e").property("~ttl", 5).from(v1).to(v2).next();
        Thread.sleep(3500);
        // See that decreasing TTL works.
        g.E(e.id()).property("~ttl", 1).iterate();
        Thread.sleep(1500);
        Assert.assertFalse(g.E(e.id()).hasNext());
        // See that increasing TTL works.
        e = g.addE("e").property("~ttl", 3).from(v1).to(v2).next(); // This should expire at 8 seconds, caught by the 6-9 second interval.
        Thread.sleep(2000);
        // Update TTL to be at the 9 second mark.
        g.E(e.id()).property("~ttl", 2).iterate();
        // Check at the 8.5 second mark.
        Thread.sleep(1500);
        Assert.assertTrue(g.E(e.id()).hasNext());
        // Check at the 9.5 second mark.
        Thread.sleep(1000);
        Assert.assertFalse(g.E(e.id()).hasNext());
    }

    private static void assertTtlAccuracy(final FireflyGraph graph) throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
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
        Assert.assertTrue(g.V(v1.id()).hasNext());
        Assert.assertTrue(g.V(v2.id()).hasNext());
        Assert.assertTrue(g.V(v3.id()).hasNext());
        Assert.assertTrue(g.V(v4.id()).hasNext());
        Assert.assertTrue(g.V(v5.id()).hasNext());
        Assert.assertTrue(g.E(v1tov2.id()).hasNext());
        Assert.assertTrue(g.E(v2tov1.id()).hasNext());
        Assert.assertTrue(g.E(v2tov3.id()).hasNext());
        Assert.assertTrue(g.E(v3tov2.id()).hasNext());
        Assert.assertTrue(g.E(v2tov3longTtl.id()).hasNext());
        Assert.assertFalse(g.V().has("~ttl").hasNext());
        Assert.assertFalse(g.E().has("~ttl").hasNext());

        // Sleep for 3.5s for TTL to kick in
        Thread.sleep(3500);
        // Assert expected TTL elements are removed and edges attached to TTL vertices too as well
        Assert.assertFalse(g.V(v1.id()).hasNext());
        Assert.assertTrue(g.V(v2.id()).hasNext());
        Assert.assertTrue(g.V(v3.id()).hasNext());
        Assert.assertFalse(g.V(v4.id()).hasNext());
        Assert.assertTrue(g.V(v5.id()).hasNext());
        Assert.assertFalse(g.E(v1tov2.id()).hasNext());
        Assert.assertFalse(g.E(v2tov1.id()).hasNext());
        Assert.assertFalse(g.E(v2tov3.id()).hasNext());
        Assert.assertTrue(g.E(v3tov2.id()).hasNext());
        Assert.assertTrue(g.E(v2tov3longTtl.id()).hasNext());

        g.E(v3tov2.id()).property("~ttl", 3).iterate();
        Thread.sleep(3500);
        Assert.assertFalse(g.E(v3tov2.id()).hasNext());
    }

    private static Configuration getConfig(final int purgeIntervalSeconds, final boolean allowTtlUpdateAnytime) {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.TTL_ENABLED_FLAG.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.TTL_PURGE_INTERVAL_SECONDS.toLowerCase(), String.valueOf(purgeIntervalSeconds));
        config.setProperty(ConfigurationHelper.Keys.TTL_UPDATE_ANYTIME_FLAG.toLowerCase(), String.valueOf(allowTtlUpdateAnytime));
        return config;
    }
}
