package com.aerospike.firefly.util;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
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
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("the following configuration keys are invalid: [aerospike.invalid]"));
        }
    }

    @Test
    public void testInvalidQueryTracingHealthcheck() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.query-tracing.threshold-ms", "0");
        try (final AerospikeConnection db = AerospikeConnection.connect(config)) {
            GraphFactory.createGraph(db, config);
            fail("Error, graph should not have opened with a failed query tracing healthcheck.");
        } catch (final IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Connection to Query Tracing endpoint failed with no response. This is most likely due to an incorrect IP or Port"));
        }
    }
}
