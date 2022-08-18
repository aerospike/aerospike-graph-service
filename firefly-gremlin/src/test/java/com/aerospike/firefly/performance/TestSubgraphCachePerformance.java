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
import org.junit.After;
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
    protected static final Configuration config;
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

    public static void openGraphCacheEnabled() {
        config.setProperty(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY.toLowerCase(), "true");
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
    public void twoHopTestCacheEnabled() throws IOException {
        openGraphCacheEnabled();
        PerfUtil.Results results = PerfUtil.runTestBatch(2, () -> {
            long startHitCount = graph.getBaseGraph().getSubgraphCache().getHitCount();
            long startMissCount = graph.getBaseGraph().getSubgraphCache().getMissCount();
            Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
            Long res = g.V(aus).out().out().dedup().count().next();
            List<Vertex> airports = g.V().has("code").sample(3).toList();
            long secondHitCount = graph.getBaseGraph().getSubgraphCache().getHitCount();
            long secondMissCount = graph.getBaseGraph().getSubgraphCache().getMissCount();
            assertTrue(secondHitCount > startHitCount);
        });
        LOG.info(results.toString());
        graph.close();
        db.close();
    }

    @Test
    public void twoHopTestCacheDisabled() throws IOException {
        openGraphCacheDisabled();
        PerfUtil.Results results = PerfUtil.runTestBatch(2, () -> {
            long startHitCount = graph.getBaseGraph().getSubgraphCache().getHitCount();
            long startMissCount = graph.getBaseGraph().getSubgraphCache().getMissCount();
            Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
            Long res = g.V(aus).out().out().dedup().count().next();
            List<Vertex> airports = g.V().has("code").sample(3).toList();
            long secondHitCount = graph.getBaseGraph().getSubgraphCache().getHitCount();
            long secondMissCount = graph.getBaseGraph().getSubgraphCache().getMissCount();
            assertEquals(startHitCount, secondHitCount);
        });
        LOG.info(results.toString());
        graph.close();
        db.close();
    }
}
