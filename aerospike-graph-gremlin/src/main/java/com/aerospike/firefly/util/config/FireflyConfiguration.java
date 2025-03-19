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
