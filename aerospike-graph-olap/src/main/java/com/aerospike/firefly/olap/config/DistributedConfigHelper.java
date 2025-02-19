package com.aerospike.firefly.olap.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class DistributedConfigHelper implements Serializable {

    private final Map<String, Object> fileConfig;
    private final Map<String, Object> olapConfig;

    private static final String OLAP_PREFIX = "aerospike.graph.olap.";
    private static final String DEBUG_DF = OLAP_PREFIX + "debug.df";
    private static final boolean DEBUG_DF_DEFAULT = false;
    private static final String PARTITIONS = OLAP_PREFIX + "partitions";

    public DistributedConfigHelper(final Map<String, Object> fireflyConfig, final Map<String, Object> olapConfig) {
        this.fileConfig = fireflyConfig;
        this.olapConfig = olapConfig;
    }

    public Configuration getFireflyConfig() {
        return new MapConfiguration(this.fileConfig);
    }

    public Configuration getOlapConfig() {
        return new MapConfiguration(this.olapConfig);
    }

    public boolean isDebugDf() {
        return getOlapConfig().getBoolean(DEBUG_DF, DEBUG_DF_DEFAULT);
    }

    public Optional<Integer> getPartitions() {
        return Optional.ofNullable(getOlapConfig().getInteger(PARTITIONS, null));
    }
}
