package com.aerospike.firefly.bulkloader.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ObjectLoader {
    Map<String, Object> loadConfiguration(final String configPath);

    List<String> getCsvPaths(final String directory) throws IOException;
}
