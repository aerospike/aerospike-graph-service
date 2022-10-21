package com.aerospike.firefly.spark.bulkloader.integration;

import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;

import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.getConfig;

public class TestSparkBulkLoaderLinked extends TestSparkBulkLoaderBase {
    static private final String DEFAULT_CONFIG = "src/test/resources/conf/linked/config.properties";
    static private final String USE_PROVIDED_ID_FALSE_CONFIG = "src/test/resources/conf/linked/useProvidedIdFalse.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG = "src/test/resources/conf/linked/keepProvidedIdAsProperty.properties";
    static private final String DATA_MODEL = "linked";

    @Test
    public void testDataModelInitializedCorrectly() {
        Assert.assertEquals(DATA_MODEL, graph.getDataModel());
    }

    @Override
    protected Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    @Override
    protected String getDataModel() {
        return DATA_MODEL;
    }

    @Override
    protected String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    @Override
    protected String getUseProvidedEdgeIdFalseAndKeepIdFalseConfig() {
        return USE_PROVIDED_ID_FALSE_CONFIG;
    }

    @Override
    protected String getUseProvidedEdgeIdFalseKeepIdAsPropertyTrueConfig() {
        return KEEP_ID_AS_PROPERTY_CONFIG;
    }
}
