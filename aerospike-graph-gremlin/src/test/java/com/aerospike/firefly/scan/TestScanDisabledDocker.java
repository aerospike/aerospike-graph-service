/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.scan;

import com.aerospike.firefly.util.RemoteDockerTestHost;
import com.aerospike.firefly.util.exceptions.GraphError;
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
 * Tests that scan queries are disabled by default when aerospike.graph.scan.enabled=false.
 * This test is designed to run against a Docker container with the default configuration.
 */
public class TestScanDisabledDocker {
    private static final String HOST = RemoteDockerTestHost.gremlinHost();
    private static final int PORT = 8182;
    private static final String SCAN_OPTION_KEY = "aerospike.graph.scan.enabled";
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();
    private static Object testVertexId;

    @BeforeClass
    static public void beforeAll() throws Exception {
        // Add a test vertex using addV (which is not a scan)
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // First enable scan to drop existing vertices
            g.with(SCAN_OPTION_KEY, true).V().drop().iterate();
            // Add test vertices
            final Vertex v = g.addV("person").property("name", "test").next();
            testVertexId = v.id();
            g.addV("person").property("name", "test2").next();
        }
    }

    @AfterClass
    static public void afterAll() throws Exception {
        // Clean up with scan enabled
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.with(SCAN_OPTION_KEY, true).V().drop().iterate();
        }
        CLUSTER.close();
    }

    @Test
    public void testScanDisabledByDefault() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            try {
                // g.V() is a scan operation and should fail
                g.V().toList();
                Assert.fail("Scan operation should have failed when disabled by default.");
            } catch (Exception e) {
                Assert.assertTrue("Expected SCAN_NOT_ALLOWED error, got: " + e.getMessage(),
                        e.getMessage().contains(GraphError.getMessage(GraphError.SCAN_NOT_ALLOWED)));
            }
        }
    }

    @Test
    public void testScanEnabledWithOption() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // Should work when explicitly enabled via .with()
            final List<Vertex> vertices = g.with(SCAN_OPTION_KEY, true).V().toList();
            Assert.assertEquals("Should return 2 vertices", 2, vertices.size());
        }
    }

    @Test
    public void testScanEnabledWithStringOption() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // Should work with string "true"
            final List<Vertex> vertices = g.with(SCAN_OPTION_KEY, "true").V().toList();
            Assert.assertEquals("Should return 2 vertices", 2, vertices.size());
        }
    }

    @Test
    public void testQueryByIdStillWorks() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // Query by ID is not a scan and should work
            final Vertex v = g.V(testVertexId).next();
            Assert.assertNotNull("Query by ID should work even when scans are disabled", v);
            Assert.assertEquals("test", v.value("name"));
        }
    }

    @Test
    public void testAddVertexStillWorks() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            // addV is not a scan and should work
            final Vertex v = g.addV("test").property("key", "value").next();
            Assert.assertNotNull("addV should work even when scans are disabled", v);
            // Clean up this vertex
            g.V(v.id()).drop().iterate();
        }
    }
}
