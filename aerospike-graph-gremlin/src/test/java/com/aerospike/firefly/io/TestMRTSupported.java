package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.AbstractFireflySuite.exited;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.HTTP_ENABLED;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_ENABLED_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.loadFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

// to run this test need AeroSpike database 8+ with strong consistency
public class TestMRTSupported {

    @Test
    public void testTransactionSupport() {
        FireflyGraph.EXIT_MANAGER = new AbstractFireflySuite.ExitManagerTest();

        final Configuration config = loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(HTTP_ENABLED.toLowerCase(), "false");

        // should not fail
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().count().iterate();
            // should be default supernode limit for test config
            assertEquals(6553, graph.getBaseGraph().ON_RECORD_ID_LIMIT);
        }
        assertFalse(exited);

        config.setProperty(MRT_ENABLED_FLAG, "true");
        // should not fail
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().count().iterate();
            // supernode limit should be more strict
            assertEquals(1023, graph.getBaseGraph().ON_RECORD_ID_LIMIT);
        }
        assertFalse(exited);
    }
}
