package com.aerospike.firefly.io;

import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.Util;
import com.google.common.cache.CacheStats;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AbstractSubgraphTest {
    protected static Configuration config;
    final private Logger LOG;
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY.toLowerCase(), "false");
    }

    static final ConcurrentHashMap<UUID, CacheStats> cacheResults = new ConcurrentHashMap<>();

    @BeforeClass
    public static void preloadData() throws IOException {
        FireflyCacheGCStep.registerGCHook((cacheId, cache) -> {
            cacheResults.put(cacheId, cache.stats());
            return null;
        });
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        Assume.assumeTrue(!StarPackedGraph.isStarPackedGraph(graph));
        loadAirRoutes();
        graph.close();
        db.close();
    }

    @AfterClass
    public static void closeGraphClearData() {
        FireflyCacheGCStep.clearGCHooks();

        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        Util.cleanAndVerifyGraph(graph);
        graph.close();
        db.close();
    }

    protected static GraphTraversalSource g;
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

    public AbstractSubgraphTest() {
        LOG = LoggerFactory.getLogger(this.getClass());
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
}
