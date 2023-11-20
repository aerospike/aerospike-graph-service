package com.aerospike.firefly.runtime.server.validconnection;

import com.aerospike.firefly.benchmark.BenchmarkTestUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class ValidConnectionTest {
    // Sample usage: mvn test -Dfirefly.host=172.17.0.3 -Ddocker.benchmark=1 -Dtest=ValidConnectionTest -DfailIfNoTests=false --no-transfer-progress
    private static final Logger LOG = LoggerFactory.getLogger(ValidConnectionTest.class);
    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);

    @Test
    public void testConnection() throws Exception {
        LOG.info("Creating the Cluster with host {} and port {}.", HOST, PORT);
        final Cluster cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource.");
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        LOG.info("Running count to test connection.");
        g.V().count().next();

        LOG.info("Closing GraphTraversalSource.");
        g.close();

        LOG.info("Closing Cluster.");
        cluster.close();
    }
}
