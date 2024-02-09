package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

public class TestInvalidConfig {
    @Test
    public void testInvalidConfig() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.invalid", "invalid");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            fail("Error, graph should not have opened with invalid config.");
        } catch (Exception e) {
        }
    }
}
