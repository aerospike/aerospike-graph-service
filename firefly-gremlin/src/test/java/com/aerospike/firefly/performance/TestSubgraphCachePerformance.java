package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.PerfUtil;
import com.aerospike.firefly.util.Util;
import org.apache.commons.configuration2.Configuration;
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

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestSubgraphCachePerformance {
    protected static Configuration config;
    final private Logger LOG;
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY.toLowerCase(), "false");
    }

    public TestSubgraphCachePerformance() {
        LOG = LoggerFactory.getLogger(this.getClass());
    }

    @BeforeClass
    public static void preloadData() throws IOException {
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        loadAirRoutes();
        graph.close();
        db.close();
    }
    public static void openGraphCacheEnabledSync() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.ASYNC_SUBGRAPH_CACHE.toLowerCase(), "false");

        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }
    public static void openGraphCacheEnabledAsync() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.ASYNC_SUBGRAPH_CACHE.toLowerCase(), "true");

        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    public static void openGraphCacheDisabled() {
        config.setProperty(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY.toLowerCase(), "false");
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }


    @AfterClass
    public static void closeGraphClearData() {
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        Util.clearGraph(graph);
        graph.close();
        db.close();
    }

    private static GraphTraversalSource g;
    private static final File tempFile;
    private static final URL airRoutesUrl;

    static {
        try {
            airRoutesUrl = new URL(AIR_ROUTES_50K_URL);
            tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void loadAirRoutes() throws IOException {
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
        g = graph.traversal();
        g.V().drop().iterate();
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
    }


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

    @Test
    public void twoHopTestCacheDisabled() throws IOException {
        openGraphCacheDisabled();
        PerfUtil.Results results = PerfUtil.runTestBatch(10, () -> {
//            long startHitCount = SubgraphCache.getHitCount();
//            long startMissCount = SubgraphCache.getMissCount();
            Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
            Long res = g.V(aus).out().out().dedup().count().next();
            List<Vertex> airports = g.V().has("code").sample(3).toList();
//            long secondHitCount = SubgraphCache.getHitCount();
//            long secondMissCount = SubgraphCache.getMissCount();
//            assertEquals(startHitCount, secondHitCount);
        });
        LOG.info(results.toString());
        graph.close();
        db.close();
    }
}
