package com.aerospike.firefly.call;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestFireflyBulkLoaderCallEntrypointRemote {
    private static final String HOST = "172.17.0.1";
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);

    @Test
    public void testRemoteEntrypoint() {
        final Cluster cluster = BUILDER.create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        Assert.assertEquals("Success", g.call("bulk-load").
                with("config", "/opt/aerospike-firefly/etc/config.properties").next());
    }
}
