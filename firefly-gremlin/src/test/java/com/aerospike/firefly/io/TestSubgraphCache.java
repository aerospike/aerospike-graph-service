package com.aerospike.firefly.io;

import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestSubgraphCache extends AbstractSubgraphTest {
    protected Logger LOG;


    @Test
    public void twoHopTest() throws IOException {
        openGraphCacheEnabledSync();
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        GraphTraversal<Vertex, Long> traversal = g.V(aus).out().out().dedup().count();
        ((Traversal.Admin) traversal).applyStrategies();
        System.out.println(traversal.toString());
        traversal.next();
        List<Vertex> airports = g.V().has("code").sample(3).toList();
    }

    @Test
    public void testDoesNotTriggerOnScans() {
        openGraphCacheEnabledSync();
        GraphTraversal<Vertex, Vertex> traversal = g.V().has("code", "AUS").out().out().dedup();
        Optional<UUID> tId = FireflyTraversalCacheStrategy.Util.idFromTraversal(((Traversal.Admin<?, ?>) traversal));
        Vertex x = traversal.next();
        if (tId.isPresent())
            fail("CacheStep not preset");
    }

    @Test
    public void twoHopTestCacheDisabled() {
        openGraphCacheDisabled();
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        GraphTraversal<Vertex, Long> traversal = g.V(aus).out().out().dedup().count();
        ((Traversal.Admin<?, ?>) traversal).applyStrategies();
        Optional<UUID> tId = FireflyTraversalCacheStrategy.Util.idFromTraversal(((Traversal.Admin<?, ?>) traversal));
        if (tId.isPresent())
            fail("should not have a traversal cache Id when disabled");
        graph.close();
        db.close();
    }

    @Test
    public void twoHopTestCacheEnabled() {
        openGraphCacheEnabledSync();
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        GraphTraversal<Vertex, Long> outoutCountTraversal = g.V(aus).out().out().dedup().count();
        ((Traversal.Admin<?, ?>) outoutCountTraversal).applyStrategies();
        Optional<UUID> tId = FireflyTraversalCacheStrategy.Util.idFromTraversal(((Traversal.Admin<?, ?>) outoutCountTraversal));
        if (tId.isEmpty())
            fail("should have a traversal cache Id");
        Long count = outoutCountTraversal.next();
        assertTrue("no cache hits", cacheResults.get(tId.get()).hitCount() > 0);

        List<Vertex> airports = g.V().has("code").sample(3).toList();

        graph.close();
        db.close();
    }
}
