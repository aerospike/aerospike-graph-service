package com.aerospike.firefly.bulkloader.storage;

import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.util.Set;

public interface ObjectLoader {
    Configuration loadConfiguration(final String configPath);
    Set<String> getObjectList(final String directory) throws RuntimeException, IOException;
}
