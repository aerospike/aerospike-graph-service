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

package com.aerospike.firefly.util.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class FireflyConfiguration extends MapConfiguration {

    private FireflyConfiguration(final Map<String, Object> configurationMap) {
        super(configurationMap);
    }

    static public FireflyConfiguration fromConfiguration(final Configuration configuration) {
        if (configuration instanceof FireflyConfiguration) {
            return (FireflyConfiguration) configuration;
        }
        final HashMap<String, Object> configurationMap = new HashMap<>();
        for (final Iterator<String> keyIterator = configuration.getKeys(); keyIterator.hasNext(); ) {
            final String configKey = keyIterator.next();
            configurationMap.put(configKey.toLowerCase(), configuration.getProperty(configKey));
        }
        return new FireflyConfiguration(configurationMap);
    }
}
