package com.aerospike.firefly.phantom_edges;


import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestPhantomEdges {

    // This test expects that an aerospike-graph docker instance is running in GCP and a bulk load has completed
    // where one or more of the works was killed mid job. This will leave phantom edges in the graph which we will find.

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(6);

    @Test
    public void findAllEdges() throws Exception {
        final DriverRemoteConnection driverRemoteConnection = DriverRemoteConnection.using("localhost", 8182, "g");
        final GraphTraversalSource g = traversal().withRemote(driverRemoteConnection);
        System.out.println("Starting calculations.");
        EXECUTOR_SERVICE.submit(() -> {
            final long outECount = g.with("evaluationTimeout", 30 * 60 * 1000).V().outE().count().next();
            System.out.println("outECount: " + outECount);
        });
        EXECUTOR_SERVICE.submit(() -> {
            final long inECount = g.with("evaluationTimeout", 30 * 60 * 1000).V().inE().count().next();
            System.out.println("inECount: " + inECount);
        });
        EXECUTOR_SERVICE.submit(() -> {
            final long outCount = g.with("evaluationTimeout", 30 * 60 * 1000).V().out().count().next();
            System.out.println("outCount: " + outCount);
        });
        EXECUTOR_SERVICE.submit(() -> {
            final long inCount = g.with("evaluationTimeout", 30 * 60 * 1000).V().in().count().next();
            System.out.println("inCount: " + inCount);
        });
        EXECUTOR_SERVICE.submit(() -> {
            final long eCount = g.with("evaluationTimeout", 30 * 60 * 1000).E().count().next();
            System.out.println("eCount: " + eCount);
        });
        EXECUTOR_SERVICE.submit(() -> {
            final long vCount = g.with("evaluationTimeout", 30 * 60 * 1000).V().count().next();
            System.out.println("vCount: " + vCount);
        });
        EXECUTOR_SERVICE.shutdown();
        EXECUTOR_SERVICE.awaitTermination(30 * 60, java.util.concurrent.TimeUnit.SECONDS);
        driverRemoteConnection.close();
    }
}
