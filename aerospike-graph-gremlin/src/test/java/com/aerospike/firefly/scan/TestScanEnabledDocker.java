package com.aerospike.firefly.scan;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Tests that scan queries work by default when aerospike.graph.scan.enabled=true in config.
 * This test is designed to run against a Docker container with scan enabled in configuration.
 */
public class TestScanEnabledDocker {
    private static final String HOST = "172.17.0.1";
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();

    @BeforeClass
    static public void beforeAll() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // Scan is enabled by config, so this should work
            g.V().drop().iterate();
            // Add test vertices
            g.addV("person").property("name", "test").next();
            g.addV("person").property("name", "test2").next();
        }
    }

    @AfterClass
    static public void afterAll() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
        }
        CLUSTER.close();
    }

    @Test
    public void testScanEnabledByConfig() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // g.V() should work because scan is enabled in config
            final List<Vertex> vertices = g.V().toList();
            Assert.assertEquals("Should return 2 vertices", 2, vertices.size());
        }
    }

    @Test
    public void testScanCanBeDisabledWithOption() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            try {
                // Should fail when explicitly disabled via .with() even if config allows it
                g.with("aerospike.graph.scan.enabled", false).V().toList();
                Assert.fail("Scan operation should have failed when disabled via .with()");
            } catch (Exception e) {
                Assert.assertTrue("Expected SCAN_NOT_ALLOWED error, got: " + e.getMessage(),
                        e.getMessage().contains("Scan queries are not enabled"));
            }
        }
    }
}
