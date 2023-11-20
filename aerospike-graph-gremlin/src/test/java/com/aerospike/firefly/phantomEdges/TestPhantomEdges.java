package com.aerospike.firefly.phantomEdges;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

//@todo: move to bulk loader maven module
/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestPhantomEdges {

    // This test expects that an aerospike-graph docker instance is running in GCP and a bulk load has completed
    // where one or more of the works was killed mid job. This will leave phantom edges in the graph which we will find.

    @Test
    public void findAllEdges() throws Exception {
        final DriverRemoteConnection driverRemoteConnection = DriverRemoteConnection.using("localhost", 8182, "g");
        final GraphTraversalSource g = traversal().withRemote(driverRemoteConnection);

        // Get total edges, edges from the left, and edges from the right.
        final long outECount = g.with("evaluationTimeout", 30 * 60 * 1000).V().outE().count().next();
        final long inECount = g.with("evaluationTimeout", 30 * 60 * 1000).V().inE().count().next();
        final long eCount = g.with("evaluationTimeout", 30 * 60 * 1000).E().count().next();

        // Compare to expected value.
        Assert.assertEquals(eCount, 14000000L);
        Assert.assertEquals(inECount, 14000000L);
        Assert.assertEquals(outECount, 14000000L);
        driverRemoteConnection.close();
    }
}
