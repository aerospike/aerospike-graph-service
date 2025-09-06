package com.aerospike.firefly.io.aerospike.indexes;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyIndexes {
    static final private Configuration SETUP_CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    @Before
    public void beforeEach() throws Exception {
        try (final FireflyGraph graph = FireflyGraph.open(SETUP_CONFIG)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    @AfterClass
    static public void afterAll() {
        try (final FireflyGraph graph = FireflyGraph.open(SETUP_CONFIG)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    @Test
    public void testConfiguringIndexes() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, "one,two,three");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_STRING_INDEXES, "one,four");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_NUMERIC_INDEXES, "two,five");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeConnection db = graph.getBaseGraph();
            final List<String> existingIndexes =
                    AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                            .map(Map.Entry::getKey).collect(Collectors.toList());
            Assert.assertTrue(existingIndexes.contains("0_VP_one_STRING"));
            Assert.assertTrue(existingIndexes.contains("0_VP_one_NUMERIC"));
            Assert.assertTrue(existingIndexes.contains("0_VP_two_STRING"));
            Assert.assertTrue(existingIndexes.contains("0_VP_two_NUMERIC"));
            Assert.assertTrue(existingIndexes.contains("0_VP_three_STRING"));
            Assert.assertTrue(existingIndexes.contains("0_VP_three_NUMERIC"));
            Assert.assertTrue(existingIndexes.contains("0_VP_four_STRING"));
            Assert.assertFalse(existingIndexes.contains("0_VP_four_NUMERIC"));
            Assert.assertFalse(existingIndexes.contains("0_VP_five_STRING"));
            Assert.assertTrue(existingIndexes.contains("0_VP_five_NUMERIC"));
        }
    }

    @Test
    public void sanityCheckTestingMethodWorks() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final var g = graph.traversal();
            for (int i = 0; i < 10; i++) {
                g.addV().property("foo", i).iterate();
            }
            g.V().has("foo", 5).iterate();
            Assert.fail("Scan timeout should have errored for testing purposes but did not.");
        } catch (final AerospikeGraphException e) {
            Assert.assertEquals(GraphError.SCAN_NOT_ALLOWED.code, e.errorCode);
        }
    }

    @Test
    public void testBasicIndexFilter() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_STRING_INDEXES, "string");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_NUMERIC_INDEXES, "numeric");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Sleep to wait for sindex creation
            Thread.sleep(5000);
            final var g = graph.traversal();
            for (long i = 0; i < 50; i++) {
                g.addV().property("string", "string" + i).iterate();
                g.addV().property("string", i).iterate();
                g.addV().property("numeric", i).iterate();
                g.addV().property("numeric", "string" + i).iterate();
            }
            Assert.assertFalse(g.V().has("string", "string25").toList().isEmpty());
            Assert.assertFalse(g.V().has("numeric", 25).toList().isEmpty());
        }
    }

    @Test
    public void testCanFilterNumericOnStringIndexedKey() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_STRING_INDEXES, "string");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Sleep to wait for sindex creation
            Thread.sleep(5000);
            final var g = graph.traversal();
            for (long i = 0; i < 50; i++) {
                g.addV().property("string", "string" + i).iterate();
                g.addV().property("string", i).iterate();
            }
            try {
                Assert.assertFalse(g.V().has("string", 25).toList().isEmpty());
                Assert.fail("Scan did not fail when it should have.");
            } catch (final AerospikeGraphException e) {
                Assert.assertEquals(GraphError.SCAN_NOT_ALLOWED.code, e.errorCode);
            }
        }
        config.clearProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final var g = graph.traversal();
            Assert.assertFalse(g.V().has("string", 25).toList().isEmpty());
        }
    }

    @Test
    public void testCanFilterStringOnNumericIndexedKey() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_NUMERIC_INDEXES, "numeric");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Sleep to wait for sindex creation
            Thread.sleep(5000);
            final var g = graph.traversal();
            for (long i = 0; i < 50; i++) {
                g.addV().property("numeric", "string" + i).iterate();
                g.addV().property("numeric", i).iterate();
            }
            try {
                Assert.assertFalse(g.V().has("numeric", "string25").toList().isEmpty());
                Assert.fail("Scan did not fail when it should have.");
            } catch (final AerospikeGraphException e) {
                Assert.assertEquals(GraphError.SCAN_NOT_ALLOWED.code, e.errorCode);
            }
        }
        config.clearProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final var g = graph.traversal();
            Assert.assertFalse(g.V().has("numeric", "string25").toList().isEmpty());
        }
    }

    @Test
    public void testSelectsHighestStringCardinalityProperly() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_STRING_INDEXES, "low,high");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Sleep to wait for sindex creation
            Thread.sleep(5000);
            final var g = graph.traversal();
            for (long i = 0; i < 50; i++) {
                // "low" cardinality is of the correct type but lower cardinality.
                // "high" cardinality is of the incorrect type but higher cardinality.
                // Make sure it doesn't scan the higher cardinality and instead indexes lower cardinality.
                g.addV().property("low", "foo").property("high", i).iterate();
                g.addV().property("low", "foo").property("high", "string" + i).iterate();
            }
            Assert.assertFalse(g.V().has("low", "foo").has("high", 25).toList().isEmpty());
        }
    }

    @Test
    public void testSelectsHighestNumericCardinalityProperly() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_NUMERIC_INDEXES, "low,high");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Sleep to wait for sindex creation
            Thread.sleep(5000);
            final var g = graph.traversal();
            for (long i = 0; i < 50; i++) {
                // "low" cardinality is of the correct type but lower cardinality.
                // "high" cardinality is of the incorrect type but higher cardinality.
                // Make sure it doesn't scan the higher cardinality and instead indexes lower cardinality.
                g.addV().property("low", 1).property("high", "string" + i).iterate();
                g.addV().property("low", 1).property("high", i).iterate();
            }
            Assert.assertFalse(g.V().has("low", 1).has("high", "string25").toList().isEmpty());
        }
    }

    @Test
    public void testMultiPropertyMixedTypeCardinalitySelection() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.SCAN_QUERY_ALLOWED, "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_NUMERIC_INDEXES, "low,high");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Sleep to wait for sindex creation
            Thread.sleep(5000);
            final var g = graph.traversal();
            for (long i = 0; i < 50; i++) {
                // "low" cardinality is of the correct type but lower cardinality.
                // "high" cardinality has an indexed and non-indexed type.
                // Make sure it doesn't scan the higher cardinality when the predicate type mismatches
                // and instead indexes lower cardinality.
                g.addV().property("low", 1)
                        .property(VertexProperty.Cardinality.list, "high", "string" + i)
                        .property(VertexProperty.Cardinality.list, "high", i).iterate();
            }
            Assert.assertFalse(g.V().has("low", 1).has("high", "string25").toList().isEmpty());
            Assert.assertFalse(g.V().has("low", 1).has("high", 25).toList().isEmpty());
        }
    }
}
