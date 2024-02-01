package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class CacheTestsUtils {

    static FireflyGraph getCacheDisabledFirefly() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        config.clearProperty(ON_RECORD_ID_LIMIT.toLowerCase());
        return FireflyGraph.open(config);
    }

    static FireflyGraph getCacheWithSizeFirefly(final int cacheSize) {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "true");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), cacheSize);
        return FireflyGraph.open(config);
    }
}
