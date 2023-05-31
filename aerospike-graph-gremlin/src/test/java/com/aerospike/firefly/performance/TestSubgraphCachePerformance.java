package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AbstractSubgraphTest;
import com.aerospike.firefly.util.PerfUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestSubgraphCachePerformance extends AbstractSubgraphTest {
    final Logger LOG = LoggerFactory.getLogger(TestSubgraphCachePerformance.class);


    @Test
    public void twoHopTest() throws IOException {
        openGraphCacheDisabled();
        PerfUtil.Results noCacheResults = PerfUtil.runTestBatch(10, () -> {
            List<Vertex> res = g.V(1).out().out().dedup().toList();
        });
        LOG.info("No cache");
        LOG.info(noCacheResults.toString());
        graph.close();
        db.close();
        openGraphCacheEnabledSync();
        PerfUtil.Results syncCacheResults = PerfUtil.runTestBatch(10, () -> {
            List<Vertex> res = g.V(1).out().out().dedup().toList();
        });
        LOG.info("Sync cache");
        LOG.info(syncCacheResults.toString());
        graph.close();
        db.close();
        openGraphCacheEnabledAsync();
        PerfUtil.Results asyncCacheResults = PerfUtil.runTestBatch(10, () -> {
            List<Vertex> res = g.V(1).out().out().dedup().toList();
        });
        LOG.info("Async cache");
        LOG.info(asyncCacheResults.toString());
        graph.close();
        db.close();
    }

}
