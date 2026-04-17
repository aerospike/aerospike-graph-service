/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

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
