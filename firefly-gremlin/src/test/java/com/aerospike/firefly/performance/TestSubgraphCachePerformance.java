package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AbstractSubgraphTest;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.PerfUtil;
import com.aerospike.firefly.util.Util;
import com.google.common.cache.CacheStats;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.minlog.Log;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.junit.Assert.*;

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
