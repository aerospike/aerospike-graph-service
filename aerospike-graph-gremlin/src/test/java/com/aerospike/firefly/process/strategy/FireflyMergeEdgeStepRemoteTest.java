package com.aerospike.firefly.process.strategy;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.aerospike.firefly.process.strategy.FireflyMergeEdgeStepTest.concurrentWithOptionsTest;
import static com.aerospike.firefly.process.strategy.FireflyMergeEdgeStepTest.concurrentWritingTest;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class FireflyMergeEdgeStepRemoteTest {
    private static final String HOST = "172.17.0.1";
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
    public void testConcurrentWriting() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER));
             final GraphTraversalSource g2 = traversal().withRemote(DriverRemoteConnection.using(CLUSTER));
             final GraphTraversalSource g3 = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            concurrentWritingTest(g, g2, g3);
        }
    }

    @Test
    public void testConcurrentWithOptions() throws Exception {
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(CLUSTER))) {
            concurrentWithOptionsTest(g);
        }
    }
}
