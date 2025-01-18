package com.aerospike.firefly.olap.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class DistributedConfigHelper implements Serializable {

    private final Map<String, Object> fileConfig;

    public DistributedConfigHelper(final Map<String, Object> fileConfig) {
        this.fileConfig = fileConfig;
    }

    public Configuration getFireflyConfig() {
        return new MapConfiguration(this.fileConfig);
    }
}
