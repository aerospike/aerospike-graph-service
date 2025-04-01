package com.aerospike.firefly.io;

import com.aerospike.firefly.benchmark.BenchmarkTestUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTLSIntegration {
    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build()
            .addContactPoint(HOST)
            .port(PORT)
            .enableSsl(false); // This isn't AGS <-> Aerospike TLS

    @Test
    public void testConnection() throws Exception {
        final Cluster cluster = BUILDER.create();
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster))) {
            g.addV("test-tls").iterate();
            Assert.assertEquals(1, (long) g.V().hasLabel("test-tls").count().next());
        }
    }
}
