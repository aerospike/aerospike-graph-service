package com.aerospike.firefly.call;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestFireflyBulkLoaderCallEntrypointRemoteFailsWithoutSpark {
    private static final String HOST = "172.17.0.1";
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();

    @Test
    public void testRemoteEntryPoint() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("bulk-load").
                        with("aerospike.graphloader.config", "/opt/aerospike-firefly/etc/config.properties").next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Error, to use the bulk loader via the call API, use the docker image with bulk loader support."));
            }
        }
    }

    @Test
    public void testRemoteEntryPointNoConfig() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("bulk-load")
                        .with("aerospike.graphloader.vertices", "/opt/aerospike-firefly/etc/sampledata/vertices")
                        .with("aerospike.graphloader.edges", "/opt/aerospike-firefly/etc/sampledata/edges").next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Error, to use the bulk loader via the call API, use the docker image with bulk loader support."));
            }
        }
    }

    @Test
    public void testRemoteEntryPointS3() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("bulk-load")
                        .with("aerospike.graphloader.vertices", "s3://gha-ci-firefly-bulkloader/vertices/")
                        .with("aerospike.graphloader.edges", "s3://gha-ci-firefly-bulkloader/edges/")
                        .with("aerospike.graphloader.remote.user", System.getenv("AWS_ACCESS_KEY_ID"))
                        .with("aerospike.graphloader.remote.passkey", System.getenv("AWS_SECRET_ACCESS_KEY"))
                        .next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Error, to use the bulk loader via the call API, use the docker image with bulk loader support."));
            }
        }
    }

    @Test
    public void testRemoteEntryPointGcs() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            try {
                g.call("bulk-load")
                        .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices/")
                        .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges/")
                        .with("aerospike.graphloader.remote.user", System.getenv("GCS_PRIVATE_KEY_ID"))
                        .with("aerospike.graphloader.remote.passkey", System.getenv("GCS_PRIVATE_KEY"))
                        .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                        .next();
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Error, to use the bulk loader via the call API, use the docker image with bulk loader support."));
            }
        }
    }
}
