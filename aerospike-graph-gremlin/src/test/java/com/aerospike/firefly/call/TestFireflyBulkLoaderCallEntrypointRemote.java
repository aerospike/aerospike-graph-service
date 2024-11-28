package com.aerospike.firefly.call;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestFireflyBulkLoaderCallEntrypointRemote {
    private static final String HOST = "172.17.0.1";
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Cluster CLUSTER = BUILDER.create();

    @Test
    public void testRemoteEntryPoint() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            Assert.assertEquals("Success", g.with("evaluationTimeout", 5 * 60 * 1000).
                    call("aerospike.graphloader.admin.bulk-load.load").
                    with("aerospike.graphloader.config", "/opt/aerospike-graph/etc/config.properties").next());
        }
    }

    @Test
    public void testOLTP() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("34.123.37.227", 8182))) {
            Instant instant = Instant.now();
            var x = g.with("evaluationTimeout", 60 * 60 * 1000).V().hasLabel("Person").count().toList();
            System.out.println(x);
            System.out.println("Time : " + (Instant.now().toEpochMilli() - instant.toEpochMilli()));
        }
    }

    @Test
    public void testOLAP() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("34.123.37.227", 8182))) {
            Instant instant = Instant.now();
            var x = g.with("evaluationTimeout", 60 * 60 * 1000).withComputer().V().hasLabel("Person").count().toList();
            System.out.println(x);
            System.out.println("Time : " + (Instant.now().toEpochMilli() - instant.toEpochMilli()));
        }
    }

    @Test
    public void testRemoteEntryPointNoConfig() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            Assert.assertEquals("Success", g.with("evaluationTimeout", 5 * 60 * 1000).
                    call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "/opt/aerospike-graph/etc/sampledata/vertices")
                    .with("aerospike.graphloader.edges", "/opt/aerospike-graph/etc/sampledata/edges")
                    .next());
        }
    }

    @Test
    public void testRemoteEntryPointS3() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            Assert.assertEquals("Success", g.with("evaluationTimeout", 5 * 60 * 1000).
                    call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "s3://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "s3://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("AWS_ACCESS_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("AWS_SECRET_ACCESS_KEY"))
                    .next());
        }
    }

    @Test
    public void testRemoteEntryPointGcs() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            g.V().drop().iterate();
            g.E().drop().iterate();
            Assert.assertEquals("Success", g.with("evaluationTimeout", 5 * 60 * 1000).
                    call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                    .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                    .next());
        }
    }
}
