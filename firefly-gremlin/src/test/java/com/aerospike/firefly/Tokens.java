package com.aerospike.firefly;

import com.aerospike.firefly.io.impl.relational.linked.LinkedGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class Tokens {
    private Tokens() {
    }

    //Path is relative to module, ie ./firefly-gremlin/
    private static final Map<String, Path> INTEGRATION_TEST_CONFIGURATIONS = ImmutableMap.of(
            LinkedGraph.DATA_MODEL, Path.of("../conf/integration-test-settings-linked.properties"),
            PackedGraph.DATA_MODEL, Path.of("../conf/integration-test-settings-packed.properties"),
            StarPackedGraph.DATA_MODEL, Path.of("../conf/integration-test-settings-star-packed.properties")
    );

    public static final Path INTEGRATION_TEST_PROPERTIES;
    public static final String AIR_ROUTES_50K_URL = "https://raw.githubusercontent.com/krlawrence/graph/master/sample-data/air-routes-latest.graphml";


    static {
        // Default to linked.
        final String integrationTestProperties = System.getProperty("integration.test.properties");
        final Path linked = Path.of("../conf/integration-test-settings-starpacked.properties");
        if (System.getProperty("integration.test.properties") != null) {
            INTEGRATION_TEST_PROPERTIES = INTEGRATION_TEST_CONFIGURATIONS.getOrDefault(integrationTestProperties, linked);
        } else {
            INTEGRATION_TEST_PROPERTIES = linked;
        }
    }
}
