package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class Tokens {
    private Tokens() {
    }

    //Path is relative to module, ie ./aerospike-graph-gremlin/
    private static final Map<String, Path> INTEGRATION_TEST_CONFIGURATIONS = ImmutableMap.of(
            PackedGraph.DATA_MODEL, Path.of("../conf/integration-test-settings-packed.properties"),
            PackedGraph.DATA_MODEL + "-sindex", Path.of("../conf/integration-test-settings-packed-sindex.properties")
    );

    public static final Path INTEGRATION_TEST_PROPERTIES;
    public static final String AIR_ROUTES_50K_URL = "https://raw.githubusercontent.com/krlawrence/graph/master/sample-data/air-routes-latest.graphml";


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
