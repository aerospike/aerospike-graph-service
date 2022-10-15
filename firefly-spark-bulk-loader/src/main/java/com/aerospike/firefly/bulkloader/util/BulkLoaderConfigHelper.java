package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class BulkLoaderConfigHelper {
    public static final String EDGE_DIRECTORY_KEY = "edge_directory";
    public static final String VERTEX_DIRECTORY_KEY = "vertex_directory";
    public static final String IGNORE_ELEMENT_CREATION_FAILED = "ignore_element_creation_failed";
    public static final String IGNORE_PARSE_FAILED_PROPERTIES = "ignore_parse_failed_properties";

    private static final Map<String, String> DEFAULT_VALUES = new HashMap<>() {{
        put(IGNORE_ELEMENT_CREATION_FAILED, "false");
        put(IGNORE_PARSE_FAILED_PROPERTIES, "true");
    }};

    private BulkLoaderConfigHelper() {

    }

    static public String getOrDefault(final String key, final Configuration config) {
        final String loweredKey = key.toLowerCase();
        if (config.containsKey(loweredKey)) {
            return config.get(String.class, loweredKey);
        } else if (DEFAULT_VALUES.containsKey(loweredKey)) {
            return DEFAULT_VALUES.get(loweredKey);
        } else {
            throw new ConfigurationRuntimeException("No default value available for key: " + loweredKey);
        }
    }

    static public Configuration getConfig(Path path) {
        return ConfigurationHelper.loadFromFile(path);
    }
}
