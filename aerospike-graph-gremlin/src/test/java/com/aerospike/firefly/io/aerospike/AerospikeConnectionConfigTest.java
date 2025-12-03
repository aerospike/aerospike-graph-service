package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.config.FireflyConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AerospikeConnectionConfigTest {

    final static Configuration CLEAR_CONFIG = ConfigurationHelper.loadFromFile("../conf/aerospike-graph.properties");
    final static FireflyConfiguration FIREFLY_CONFIG = FireflyConfiguration.fromConfiguration(CLEAR_CONFIG);

    @Test
    public void updateConfigTest() {
        final IAerospikeClient client = mock(IAerospikeClient.class);
        when(client.getNodes()).thenReturn(new Node[3]);

        final AerospikeConnectionConfig config = new AerospikeConnectionConfig(FIREFLY_CONFIG, client);
        System.out.println(config);

        assertEquals(4, config.vertexMiscBins.size());
        assertEquals("paged", config.queryImpl);
        assertEquals(500, config.writeSocketTimeout);

        // update mutable property
        final FireflyConfiguration newConfig = FireflyConfiguration.fromConfiguration(CLEAR_CONFIG);
        newConfig.setProperty(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "123");

        final AerospikeConnectionConfig updatedConfig = config.update(newConfig, client, 1);

        assertEquals(4, updatedConfig.vertexMiscBins.size());
        assertEquals("paged", updatedConfig.queryImpl);
        assertEquals(123, updatedConfig.writeSocketTimeout);
        assertEquals(1, config.version);
    }

    @Test
    public void immutableOptionTest() {
        final IAerospikeClient client = mock(IAerospikeClient.class);
        when(client.getNodes()).thenReturn(new Node[3]);

        final AerospikeConnectionConfig config = new AerospikeConnectionConfig(FIREFLY_CONFIG, client);
        System.out.println(config);

        assertEquals(4, config.vertexMiscBins.size());
        assertEquals("paged", config.queryImpl);
        assertEquals(500, config.writeSocketTimeout);

        // immutable property update should throw exception
        final FireflyConfiguration newConfig = FireflyConfiguration.fromConfiguration(CLEAR_CONFIG);
        newConfig.setProperty(ConfigurationHelper.Keys.QUERY_IMPL, "123");

        assertThrows("", IllegalArgumentException.class, () -> config.update(newConfig, client, 2));
        assertEquals(0, config.version);
    }

    @Test
    public void saveConfigTest() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().resetConfiguration();
        }

        try (final FireflyGraph graph = FireflyGraph.open(config); final FireflyGraph graph2 = FireflyGraph.open(config)) {
            // default writeSocketTimeout
            assertEquals(500, graph.getBaseGraph().getConfig().writeSocketTimeout);
            assertEquals(500, graph2.getBaseGraph().getConfig().writeSocketTimeout);

            graph.getBaseGraph().updateConfiguration(Map.of(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "123"));
            graph2.getBaseGraph().refreshConfiguration();
            // updated writeSocketTimeout
            assertEquals(123, graph.getBaseGraph().getConfig().writeSocketTimeout);
            assertEquals(123, graph2.getBaseGraph().getConfig().writeSocketTimeout);
        }
    }

    @Test
    public void readSavedConfigTest() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        // change some configuration
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().resetConfiguration();
            graph.getBaseGraph().updateConfiguration(Map.of(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "321"));
            assertEquals(321, graph.getBaseGraph().getConfig().writeSocketTimeout);
        }

        // next instance should pick up updated writeSocketTimeout
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            assertEquals(321, graph.getBaseGraph().getConfig().writeSocketTimeout);
        }
    }

    @Test
    public void resetSavedConfigTest() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        // change some configuration
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().resetConfiguration();
            graph.getBaseGraph().updateConfiguration(Map.of(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "321"));
            assertEquals(321, graph.getBaseGraph().getConfig().writeSocketTimeout);
        }

        config.setProperty(ConfigurationHelper.Keys.CONFIG_RESET.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "3210");

        // next instance should ignore updated writeSocketTimeout
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            assertEquals(3210, graph.getBaseGraph().getConfig().writeSocketTimeout);
        }
    }

    @Test
    public void runtimeConfigUpdateTest() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().resetConfiguration();
        }

        config.setProperty(ConfigurationHelper.Keys.CONFIG_UPDATE_ENABLED.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.CONFIG_UPDATE_FREQUENCY.toLowerCase(), "400");

        // change some configuration
        try (final FireflyGraph graph = FireflyGraph.open(config); final FireflyGraph graph2 = FireflyGraph.open(config)) {
            // default config at start
            assertEquals(500, graph2.getBaseGraph().getConfig().writeSocketTimeout);

            // first graph change config
            graph.getBaseGraph().updateConfiguration(Map.of(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "3321"));
            assertEquals(3321, graph.getBaseGraph().getConfig().writeSocketTimeout);

            // some time for second graph to pick up config
            Thread.sleep(500);

            assertEquals(3321, graph2.getBaseGraph().getConfig().writeSocketTimeout);

            // and back ...
            graph2.getBaseGraph().updateConfiguration(Map.of(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "1123"));
            Thread.sleep(500);
            assertEquals(1123, graph.getBaseGraph().getConfig().writeSocketTimeout);
        }
    }

    @Test
    public void configUpdateWithCallStepTest() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.READ_SOCKET_TIMEOUT.toLowerCase(), "999");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().resetConfiguration();
        }

        config.setProperty(ConfigurationHelper.Keys.CONFIG_UPDATE_ENABLED.toLowerCase(), "true");
        config.setProperty(ConfigurationHelper.Keys.CONFIG_UPDATE_FREQUENCY.toLowerCase(), "400");

        // change some configuration
        try (final FireflyGraph graph = FireflyGraph.open(config); final FireflyGraph graph2 = FireflyGraph.open(config)) {
            // default config at start
            assertEquals(500, graph2.getBaseGraph().getConfig().writeSocketTimeout);

            // first graph change config
            final String response = (String)graph.traversal().call("aerospike.graph.admin.metadata.set-config")
                    .with(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "111")
                    .with(ConfigurationHelper.Keys.WRITE_TOTAL_TIMEOUT.toLowerCase(), "3000").next();

            assertEquals("Successfully updated configuration {aerospike.client.policy.write.totaltimeout=3000, aerospike.client.policy.write.sockettimeout=111}.",
                    response);
            assertEquals(111, graph.getBaseGraph().getConfig().writeSocketTimeout);
            assertEquals(999, graph.getBaseGraph().getConfig().readSocketTimeout);

            // some time for second graph to pick up config
            Thread.sleep(500);

            assertEquals(111, graph2.getBaseGraph().getConfig().writeSocketTimeout);
            assertEquals(999, graph.getBaseGraph().getConfig().readSocketTimeout);

            // and back ...
            graph2.traversal().call("aerospike.graph.admin.metadata.set-config")
                    .with(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT.toLowerCase(), "222").iterate();
            Thread.sleep(500);
            assertEquals(222, graph.getBaseGraph().getConfig().writeSocketTimeout);
            // other properties is not changed
            assertEquals(999, graph.getBaseGraph().getConfig().readSocketTimeout);
        }
    }

}
