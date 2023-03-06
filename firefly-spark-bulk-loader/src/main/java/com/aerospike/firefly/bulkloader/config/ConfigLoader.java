package com.aerospike.firefly.bulkloader.config;

import org.apache.commons.configuration2.Configuration;

public interface ConfigLoader {
    public Configuration loadConfigFile(final String directoryPath);
}
