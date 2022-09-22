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
    public static final String ID_BUFFER_KEY = "id_buffer_count";
    public static final String USE_PROVIDED_ID_KEY = "use_provided_id";
    public static final String ID_PROPERTY_NAME_KEY = "provided_id_property_name";

    private static final Map<String, String> DEFAULT_VALUES = new HashMap<>() {{
        put(EDGE_DIRECTORY_KEY, "firefly-bulk-loader/src/main/resources/sampledata/edges");
        put(VERTEX_DIRECTORY_KEY, "firefly-bulk-loader/src/main/resources/sampledata/vertexes");
        put(ID_BUFFER_KEY, "50");
        put(USE_PROVIDED_ID_KEY, "false");
        put(ID_PROPERTY_NAME_KEY, "~providedId");
    }};

    private BulkLoaderConfigHelper() {

    }

    static public String getOrDefault(String key, Configuration config) {
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
