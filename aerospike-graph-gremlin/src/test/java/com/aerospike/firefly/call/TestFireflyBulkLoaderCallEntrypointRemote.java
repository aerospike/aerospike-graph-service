package com.aerospike.firefly.call;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.util.BulkLoadTestUtil.waitForBulkLoad;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestFireflyBulkLoaderCallEntrypointRemote {
    private static final String HOST = "localhost";
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();

    @Test
    public void testRemoteEntryPoint() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            g.call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.config", "/opt/aerospike-graph/etc/config.properties")
                    .next();
            waitForBulkLoad(g);
        }
    }

    @Test
    public void testRemoteEntryPointNoConfig() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            g.call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "/opt/aerospike-graph/etc/sampledata/vertices")
                    .with("aerospike.graphloader.edges", "/opt/aerospike-graph/etc/sampledata/edges")
                    .next();
            waitForBulkLoad(g);
        }
    }

    @Test
    public void testRemoteEntryPointS3() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            g.with("evaluationTimeout", 20000).call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "s3://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "s3://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("AWS_ACCESS_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("AWS_SECRET_ACCESS_KEY"))
                    .next();
            waitForBulkLoad(g);
        }
    }

    @Test
    public void testRemoteEntryPointGcs() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            g.with("evaluationTimeout", 20000).call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                    .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                    .next();
            waitForBulkLoad(g);
        }
    }

    @Test
    public void testRemoteEvaluationTimeoutExceeded() {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            g.with("evaluationTimeout", 50).call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "/opt/aerospike-graph/etc/sampledata/vertices")
                    .with("aerospike.graphloader.edges", "/opt/aerospike-graph/etc/sampledata/edges")
                    .iterate();
        } catch (final Exception e) {
            Assert.assertEquals("Initializing the bulk loader was interrupted. This is most likely caused by the" +
                    " specified configuration requiring more time. Please retry using the '.with(\"evaluationTimeout\")'" +
                    " traversal modifier or contact support if the problem persists.", e.getCause().getMessage());
        }
    }
}
