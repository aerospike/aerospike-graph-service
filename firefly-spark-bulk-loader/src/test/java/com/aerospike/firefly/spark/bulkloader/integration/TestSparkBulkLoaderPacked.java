package com.aerospike.firefly.spark.bulkloader.integration;

import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;

import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.getConfig;

public class TestSparkBulkLoaderPacked extends TestSparkBulkLoaderBase {
    static private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config.properties";
    static private final String USE_PROVIDED_ID_FALSE_CONFIG = "src/test/resources/conf/packed/use-provided-id-false.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG = "src/test/resources/conf/packed/keep-provided-id-as-property.properties";
    static private final String DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/config-artificial-supernode.properties";
    static private final String USE_PROVIDED_ID_FALSE_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/use-provided-id-false-artificial-supernode.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/keep-provided-id-as-property-artificial-supernode.properties";
    static private final String DATA_MODEL = "packed";

    @Test
    public void testDataModelInitializedCorrectly() {
        Assert.assertEquals(DATA_MODEL, graph.getDataModel());
    }

    @Override
    protected Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
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

    @Override
    protected String getDefaultConfigArtificialSupernode() {
        return DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE;
    }

    @Override
    protected String getUseProvidedEdgeIdFalseAndKeepIdFalseConfigArtificialSupernode() {
        return USE_PROVIDED_ID_FALSE_CONFIG_ARTIFICIAL_SUPERNODE;
    }

    @Override
    protected String getUseProvidedEdgeIdFalseKeepIdAsPropertyTrueConfigArtificialSupernode() {
        return KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE;
    }
}
