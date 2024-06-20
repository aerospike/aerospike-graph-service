package com.aerospike.firefly.runtime.metadata;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.FIREFLY_DATA_MODEL;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.PHAT_EDGE_SIZE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.SUMMARY_ENABLED_FLAG;

public class TestFireflyConfigCompatibility {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Before
    public void beforeEach() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);
        }
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final Vertex first = g.addV("first").next();
            final Vertex second = g.addV("second").next();
            g.addE("edge").from(first).to(second).iterate();
        }
    }

    @AfterClass
    public static void afterAll() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    @Test
    public void testToggleMutableConfig() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ENABLE_FIREFLY_DROP_STRATEGY, "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            Assert.assertEquals(1, (long) g.V().hasLabel("first").out().count().next());
        }
    }

    @Test
    public void testImmutablePhatEdgeSize() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        exit.expectSystemExitWithStatus(1);
        config.setProperty(PHAT_EDGE_SIZE, "99");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            Assert.fail("Graph should not start with immutable config change on key: " + PHAT_EDGE_SIZE);
        }
    }

    @Test
    public void testImmutableSummaryEnabled() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        exit.expectSystemExitWithStatus(1);
        config.setProperty(SUMMARY_ENABLED_FLAG, "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            Assert.fail("Graph should not start with immutable config change on key: " + SUMMARY_ENABLED_FLAG);
        }
    }

    @Test
    public void testImmutableDataModel() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        exit.expectSystemExitWithStatus(1);
        config.setProperty(FIREFLY_DATA_MODEL, "the-cooler-packed");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            Assert.fail("Graph should not start with immutable config change on key: " + FIREFLY_DATA_MODEL);
        }
    }
}
