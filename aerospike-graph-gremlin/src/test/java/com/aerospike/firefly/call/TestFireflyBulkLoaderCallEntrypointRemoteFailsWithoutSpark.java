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

package com.aerospike.firefly.call;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import com.aerospike.firefly.util.RemoteDockerTestHost;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestFireflyBulkLoaderCallEntrypointRemoteFailsWithoutSpark {
    private static final String HOST = RemoteDockerTestHost.gremlinHost();
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();

    @Test
    public void testRemoteEntryPoint() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").
                        with("aerospike.graphloader.config", "/opt/aerospike-graph/etc/config.properties").next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Unrecognized service: aerospike.graphloader.admin.bulk-load.load"));
            }
        }
    }

    @Test
    public void testRemoteEntryPointNoConfig() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load")
                        .with("aerospike.graphloader.vertices", "/opt/aerospike-graph/etc/sampledata/vertices")
                        .with("aerospike.graphloader.edges", "/opt/aerospike-graph/etc/sampledata/edges").next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Unrecognized service: aerospike.graphloader.admin.bulk-load.load"));
            }
        }
    }

    @Test
    public void testRemoteEntryPointS3() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load")
                        .with("aerospike.graphloader.vertices", "s3://gha-ci-firefly-bulkloader/vertices/")
                        .with("aerospike.graphloader.edges", "s3://gha-ci-firefly-bulkloader/edges/")
                        .with("aerospike.graphloader.remote-user", System.getenv("AWS_ACCESS_KEY_ID"))
                        .with("aerospike.graphloader.remote-passkey", System.getenv("AWS_SECRET_ACCESS_KEY"))
                        .next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Unrecognized service: aerospike.graphloader.admin.bulk-load.load"));
            }
        }
    }

    @Test
    public void testRemoteEntryPointGcs() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load")
                        .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices/")
                        .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges/")
                        .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                        .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                        .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                        .next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Unrecognized service: aerospike.graphloader.admin.bulk-load.load"));
            }
        }
    }
}
