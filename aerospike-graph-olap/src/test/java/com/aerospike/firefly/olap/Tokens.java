package com.aerospike.firefly.olap;

import com.aerospike.firefly.structure.FireflyGraph;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.Map;

public final class Tokens {
    private Tokens() {
    }

    //Path is relative to module, ie ./aerospike-graph-gremlin/
    private static final Map<String, Path> INTEGRATION_TEST_CONFIGURATIONS = ImmutableMap.of(
            FireflyGraph.DATA_MODEL, Path.of("../conf/integration-test-settings-packed.properties"),
            FireflyGraph.DATA_MODEL + "-sindex", Path.of("../conf/integration-test-settings-packed-sindex.properties")
    );

    public static final Path INTEGRATION_TEST_PROPERTIES;


    static {
        // Default to packed, as it is currently our 'suggested' data model.
        final String integrationTestProperties = System.getProperty("integration.test.properties");
        final Path packed = Path.of("../conf/integration-test-settings-packed.properties");
        if (System.getProperty("integration.test.properties") != null) {
            INTEGRATION_TEST_PROPERTIES = INTEGRATION_TEST_CONFIGURATIONS.getOrDefault(integrationTestProperties, packed);
        } else {
            INTEGRATION_TEST_PROPERTIES = packed;
        }
    }
}