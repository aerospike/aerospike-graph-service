package com.aerospike.firefly.tx;

import com.aerospike.firefly.util.DockerUtil;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTransactionSummary {
    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionSummary.class);
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    @Test
    public void testSandbox() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.graph.tx.enabled=true",
                "aerospike.graph.summary.ticker.interval=2000"
        };
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);

        final DriverRemoteConnection connection = DriverRemoteConnection.using("localhost", 8182);
        final GraphTraversalSource g = traversal().withRemote(connection);
        var v1 = g.addV().id().next();
        var v2 = g.addV().id().next();
        g.addE("label").from(__.V(v1)).to(__.V(v2)).next();
        Thread.sleep(10000);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        for (String l : log) {
            LOG.warn(l);
        }
        Assert.assertTrue(true);
    }
}
