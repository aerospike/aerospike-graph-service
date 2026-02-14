package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.SCAN_QUERY_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FireflyTraversalOptionsStrategyTest {

    private static final String SCAN_OPTION_KEY = ConfigurationHelper.TraversalOptions.SCAN_QUERY_ENABLED;
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        SETUP_GRAPH = FireflyGraph.open(config);
        SETUP_GRAPH.getBaseGraph().dropDatabase(null, true);
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        g.addV("person").next();
        g.addV("person").next();
    }

    @AfterClass
    public static void afterAll() {
        if (SETUP_GRAPH != null) {
            SETUP_GRAPH.getBaseGraph().dropDatabase(null, true);
            SETUP_GRAPH.close();
        }
    }

    // ==========================================
    // Config: scan.enabled = true (default)
    // ==========================================

    @Test
    public void configEnabledTrue_noOption_shouldSucceed() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "true");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final List<Vertex> vertices = g.V().toList();
            assertEquals(2, vertices.size());
        }
    }

    @Test
    public void configEnabledTrue_optionTrue_shouldSucceed() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "true");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final List<Vertex> vertices = g.with(SCAN_OPTION_KEY, true).V().toList();
            assertEquals(2, vertices.size());
        }
    }

    @Test
    public void configEnabledTrue_optionFalse_shouldFail() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "true");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final AerospikeGraphException exception = assertThrows(AerospikeGraphException.class, () -> {
                g.with(SCAN_OPTION_KEY, false).V().toList();
            });
            assertEquals(GraphError.SCAN_NOT_ALLOWED.code, exception.errorCode);
        }
    }

    // ==========================================
    // Config: scan.enabled = false
    // ==========================================

    @Test
    public void configEnabledFalse_noOption_shouldFail() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final AerospikeGraphException exception = assertThrows(AerospikeGraphException.class, () -> {
                g.V().toList();
            });
            assertEquals(GraphError.SCAN_NOT_ALLOWED.code, exception.errorCode);
        }
    }

    @Test
    public void configEnabledFalse_optionTrue_shouldSucceed() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final List<Vertex> vertices = g.with(SCAN_OPTION_KEY, true).V().toList();
            assertEquals(2, vertices.size());
        }
    }

    @Test
    public void configEnabledFalse_optionFalse_shouldFail() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final AerospikeGraphException exception = assertThrows(AerospikeGraphException.class, () -> {
                g.with(SCAN_OPTION_KEY, false).V().toList();
            });
            assertEquals(GraphError.SCAN_NOT_ALLOWED.code, exception.errorCode);
        }
    }

    // ==========================================
    // Option isolation tests
    // ==========================================

    @Test
    public void optionShouldNotLeakBetweenTraversals() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // First traversal with option enabled should succeed
            final List<Vertex> vertices1 = g.with(SCAN_OPTION_KEY, true).V().toList();
            assertEquals(2, vertices1.size());

            // Second traversal without option should fail (config says disabled)
            final AerospikeGraphException exception = assertThrows(AerospikeGraphException.class, () -> {
                g.V().toList();
            });
            assertEquals(GraphError.SCAN_NOT_ALLOWED.code, exception.errorCode);

            // Third traversal with option enabled should succeed again
            final List<Vertex> vertices3 = g.with(SCAN_OPTION_KEY, true).V().toList();
            assertEquals(2, vertices3.size());
        }
    }

    @Test
    public void optionShouldAcceptStringValues() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // String "true" should work
            final List<Vertex> vertices = g.with(SCAN_OPTION_KEY, "true").V().toList();
            assertEquals(2, vertices.size());

            // String "false" should fail
            final AerospikeGraphException exception = assertThrows(AerospikeGraphException.class, () -> {
                g.with(SCAN_OPTION_KEY, "false").V().toList();
            });
            assertEquals(GraphError.SCAN_NOT_ALLOWED.code, exception.errorCode);
        }
    }

    // ==========================================
    // Non-scan queries should not be affected
    // ==========================================

    @Test
    public void configEnabledFalse_queryById_shouldSucceed() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "false");

        // First get vertex IDs with scan enabled
        Object vertexId;
        final Configuration configForSetup = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        configForSetup.setProperty(SCAN_QUERY_ENABLED.toLowerCase(), "true");
        try (final FireflyGraph graph = FireflyGraph.open(configForSetup)) {
            final GraphTraversalSource g = graph.traversal();
            vertexId = g.V().next().id();
        }

        // Query by ID should work even with scan disabled
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final Vertex vertex = g.V(vertexId).next();
            assertTrue(vertex != null);
        }
    }
}
