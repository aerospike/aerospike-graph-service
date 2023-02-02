package com.aerospike.firefly.io;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestSubgraphCache extends AbstractSubgraphTest {
    protected Logger LOG;

    // Ignore tests until prefetch cache is reworked.
    @Test
    @Ignore
    public void twoHopTest() {
        openGraphCacheEnabledSync();
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        GraphTraversal<Vertex, Long> traversal = g.V(aus).out().out().dedup().count();
        ((Traversal.Admin) traversal).applyStrategies();
        System.out.println(traversal.toString());
        traversal.next();
        List<Vertex> airports = g.V().has("code").sample(3).toList();
    }

    @Test
    @Ignore
    public void testDoesNotTriggerOnScans() {
        openGraphCacheEnabledSync();
        GraphTraversal<Vertex, Vertex> traversal = g.V().has("code", "AUS").out().out().dedup();
        Assert.assertNotNull(graph.getBaseGraph().transactionCache.get());
        Assert.assertNotNull(graph.getBaseGraph().transactionCache.get().uuid);
        Vertex x = traversal.next();
    }

    @Test
    @Ignore
    public void twoHopTestCacheDisabled() {
        openGraphCacheDisabled();
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        GraphTraversal<Vertex, Long> traversal = g.V(aus).out().out().dedup().count();
        ((Traversal.Admin<?, ?>) traversal).applyStrategies();
        Assert.assertNotNull(graph.getBaseGraph().transactionCache.get());
        Assert.assertNotNull(graph.getBaseGraph().transactionCache.get().uuid);
        graph.close();
        db.close();
    }
}
