package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ADJACENCY_INDEX_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class CacheTestsUtils {
    static FireflyGraph getCacheEnabledAdjacencyDisabledFirefly(final Configuration config) {
        config.clearProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase());
        config.clearProperty(ADJACENCY_INDEX_ENABLED.toLowerCase());
        config.clearProperty(ON_RECORD_ID_LIMIT.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheDisabledAdjacencyEnabledFirefly(final Configuration config) {
        config.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        config.setProperty(ADJACENCY_INDEX_ENABLED.toLowerCase(), true);
        config.clearProperty(ON_RECORD_ID_LIMIT.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheDisabledAdjacencyDisabledFirefly(final Configuration config) {
        config.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        config.setProperty(ADJACENCY_INDEX_ENABLED.toLowerCase(), false);
        config.clearProperty(ON_RECORD_ID_LIMIT.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheWithSizeFirefly(final Configuration config, final int cacheSize) {
        config.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "true");
        config.setProperty(ADJACENCY_INDEX_ENABLED.toLowerCase(), true);
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), cacheSize);
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheWithSizeFireflyAdjacencyDisabled(final Configuration config, final int cacheSize) {
        config.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "true");
        config.setProperty(ADJACENCY_INDEX_ENABLED.toLowerCase(), false);
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), cacheSize);
        return FireflyGraph.open(config);
    }
}
