package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestBulkLoaderCallEntryPoint {

    @Test
    public void testBulkLoaderCallEntryPoint() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Failed to start bulk loader due to null configPath (null)", e.getMessage());
            }
        }
    }
}
