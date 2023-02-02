package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ADJACENCY_INDEX_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ID_CACHE_SIZE;

public class CacheTestsUtils {
    static FireflyGraph getCacheDefaultFirefly(final Configuration config) {
        config.clearProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase());
        config.clearProperty(ADJACENCY_INDEX_ENABLED.toLowerCase());
        config.clearProperty(ID_CACHE_SIZE.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getEdgeCacheDisabledFirefly(final Configuration config) {
        config.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");
        config.clearProperty(ADJACENCY_INDEX_ENABLED.toLowerCase());
        config.clearProperty(ID_CACHE_SIZE.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getEdgeAdjacencyIndexDisabledFirefly(final Configuration config) {
        config.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");
        config.setProperty(ADJACENCY_INDEX_ENABLED.toLowerCase(), false);
        config.clearProperty(ID_CACHE_SIZE.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheWithSizeFirefly(final Configuration config, final int cacheSize) {
        config.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "false");
        config.clearProperty(ADJACENCY_INDEX_ENABLED.toLowerCase());
        config.setProperty(ID_CACHE_SIZE.toLowerCase(), cacheSize);
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheWithSizeFireflyAdjacencyDisabled(final Configuration config, final int cacheSize) {
        config.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "false");
        config.setProperty(ADJACENCY_INDEX_ENABLED.toLowerCase(), false);
        config.setProperty(ID_CACHE_SIZE.toLowerCase(), cacheSize);
        return FireflyGraph.open(config);
    }
}
