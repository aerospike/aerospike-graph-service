package com.aerospike.firefly.bulkloader.storage;

import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.util.List;

public interface ObjectLoader {
    Configuration loadConfiguration(final String configPath);
    List<String> getCsvPaths(final String directory) throws IOException;
}
