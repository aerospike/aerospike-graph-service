package com.aerospike.firefly.bulkloader.storage;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class HybridLoader implements ObjectLoader, Serializable {
    private static HybridLoader INSTANCE = null;
    private final ObjectLoader configLoader;
    private final ObjectLoader csvLoader;

    private HybridLoader(final ObjectLoader configLoader, final ObjectLoader csvLoader) {
        this.configLoader = configLoader;
        this.csvLoader = csvLoader;
    }

    public static void setInstance(final ObjectLoader configLoader, final ObjectLoader csvLoader) {
        INSTANCE = new HybridLoader(configLoader, csvLoader);
    }

    public static synchronized HybridLoader getInstance() {
        if (INSTANCE == null) {
            // This will never happen in production - it's only a safeguard against programmer error.
            throw new RuntimeException("HybridLoader.setInstance() must be invoked before invoking HybridLoader.getInstance()");
        }
        return INSTANCE;
    }

    @Override
    public Map<String, Object> loadConfiguration(final String configPath) {
        return configLoader.loadConfiguration(configPath);
    }

    @Override
    public List<String> getCsvPaths(final String directory) throws IOException {
        return csvLoader.getCsvPaths(directory);
    }
}
