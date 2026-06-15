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

package com.aerospike.firefly.tx;

import com.aerospike.firefly.util.RemoteDockerTestHost;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTxDisabled {
    private static final String HOST = RemoteDockerTestHost.gremlinHost();
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();

    @BeforeClass
    static public void beforeAll() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
        }
    }

    @AfterClass
    static public void afterAll() {
        CLUSTER.close();
    }

    @After
    public void afterEach() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
        }
    }

    @Test
    public void testTraversal() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            final Transaction tx = g.tx();
            final GraphTraversalSource gtx = tx.begin();
            try {
                gtx.addV().iterate();
                Assert.fail("Tx operation should have failed.");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Transactions are not enabled"));
            }
        }
    }

    @Test
    public void testCommit() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            final Transaction tx = g.tx();
            final GraphTraversalSource gtx = tx.begin();
            try {
                gtx.tx().commit();
                Assert.fail("Tx operation should have failed.");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Graph does not support transactions"));
            }
        }
    }

    @Test
    public void testRollback() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            final Transaction tx = g.tx();
            final GraphTraversalSource gtx = tx.begin();
            try {
                gtx.tx().rollback();
                Assert.fail("Tx operation should have failed.");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause().getMessage().contains("Graph does not support transactions"));
            }
        }
    }
}
