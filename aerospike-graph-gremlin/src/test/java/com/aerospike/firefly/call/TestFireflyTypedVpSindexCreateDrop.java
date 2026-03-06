package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.SindexAlreadyExistsException;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestFireflyTypedVpSindexCreateDrop {
    static private final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static final int INDEX_POLL_TIMEOUT_MS = 30000;
    private static final int INDEX_POLL_INTERVAL_MS = 100;
    private FireflyGraph graph;

    @BeforeClass
    static public void beforeAll() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    @Before
    public void beforeEach() {
        graph = FireflyGraph.open(CONFIG);
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    /**
     * Polls until all expected indexes appear in the index list. Index creation is async,
     * so we need to wait for indexes to be fully built before asserting.
     */
    @SuppressWarnings("unchecked")
    private List<String> waitForIndexes(final GraphTraversalSource g, final String... expectedIndexes) {
        final long startTime = System.currentTimeMillis();
        List<String> indexes;
        while (true) {
            indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            if (Arrays.stream(expectedIndexes).allMatch(indexes::contains)) {
                return indexes;
            }
            if (System.currentTimeMillis() - startTime > INDEX_POLL_TIMEOUT_MS) {
                Assert.fail("Timed out waiting for indexes: " + Arrays.toString(expectedIndexes) +
                        ". Current indexes: " + indexes);
            }
            try {
                Thread.sleep(INDEX_POLL_INTERVAL_MS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testSindexCreation() {
        final GraphTraversalSource g = graph.traversal();
        // IndexType.STRING
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "string").next();
        List<String> indexes = waitForIndexes(g, "foo:STRING");
        Assert.assertTrue(indexes.contains("foo:STRING"));
        Assert.assertFalse(indexes.contains("foo:NUMERIC"));
        // IndexType.NUMERIC
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "numeric").next();
        indexes = waitForIndexes(g, "bar:NUMERIC");
        Assert.assertFalse(indexes.contains("bar:STRING"));
        Assert.assertTrue(indexes.contains("bar:NUMERIC"));
        // No type specified - create all supported IndexType
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "baz").next();
        indexes = waitForIndexes(g, "baz:STRING", "baz:NUMERIC");
        Assert.assertTrue(indexes.contains("baz:STRING"));
        Assert.assertTrue(indexes.contains("baz:NUMERIC"));
        // No type specified on property with one existing IndexType
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "foo").next();
        indexes = waitForIndexes(g, "foo:STRING", "foo:NUMERIC");
        Assert.assertTrue(indexes.contains("foo:STRING"));
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        // No type specified on property with all existing IndexType
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "foo").next();
            Assert.fail("Should fail when no sindexes were created.");
        } catch (final SindexAlreadyExistsException ignored) {}
        // Type specified on property with the same IndexType already existing
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when no sindexes were created.");
        } catch (final SindexAlreadyExistsException ignored) {}
    }

    @Test
    public void testCreateSanitize() {
        final GraphTraversalSource g = graph.traversal();
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "numeric").
                    with("fourth_key", "foo").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "numeric").
                    with("fourth_key", "foo").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "bar").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("fourth_key", "foo").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "edge").
                    with("property_key", "bar").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", 123).
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", 123).next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "invalidIndexType").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "NuMeRiC").next();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "STRING").next();
        List<String> indexes = waitForIndexes(g, "bar:STRING", "bar:NUMERIC");
        Assert.assertTrue(indexes.contains("bar:STRING"));
        Assert.assertTrue(indexes.contains("bar:NUMERIC"));
    }

    @Test
    public void testDropSanitize() {
        final GraphTraversalSource g = graph.traversal();
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "numeric").
                    with("fourth_key", "foo").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "numeric").
                    with("fourth_key", "foo").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("property_key", "bar").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("fourth_key", "foo").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "edge").
                    with("property_key", "bar").
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("property_key", 123).
                    with("index_type", "numeric").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", 123).next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        try {
            g.call("aerospike.graph.admin.index.drop").
                    with("element_type", "vertex").
                    with("property_key", "bar").
                    with("index_type", "invalidIndexType").next();
            Assert.fail("Should fail when params are invalid.");
        } catch (final IllegalArgumentException ignored) {}
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "NuMeRiC").next();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "STRING").next();
        List<String> indexes = waitForIndexes(g, "bar:STRING", "bar:NUMERIC");
        Assert.assertTrue(indexes.contains("bar:STRING"));
        Assert.assertTrue(indexes.contains("bar:NUMERIC"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "NUMERIC").next();
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "sTrInG").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertFalse(indexes.contains("bar:STRING"));
        Assert.assertFalse(indexes.contains("bar:NUMERIC"));
    }

    @Test
    public void testDropTypeOnExistingType() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "numeric").next();
        List<String> indexes = waitForIndexes(g, "foo:NUMERIC");
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "numeric").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertFalse(indexes.contains("foo:NUMERIC"));
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "string").next();
        indexes = waitForIndexes(g, "bar:STRING");
        Assert.assertTrue(indexes.contains("bar:STRING"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "string").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertFalse(indexes.contains("bar:STRING"));
    }

    @Test
    public void testDropTypeOnNonExistingType() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "numeric").next();
        List<String> indexes = waitForIndexes(g, "foo:NUMERIC");
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        Assert.assertFalse(indexes.contains("foo:STRING"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "string").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        Assert.assertFalse(indexes.contains("foo:STRING"));
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "string").next();
        indexes = waitForIndexes(g, "bar:STRING");
        Assert.assertTrue(indexes.contains("bar:STRING"));
        Assert.assertFalse(indexes.contains("bar:NUMERIC"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "numeric").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertTrue(indexes.contains("bar:STRING"));
        Assert.assertFalse(indexes.contains("bar:NUMERIC"));
    }

    @Test
    public void testDropAllOnExistingType() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "numeric").next();
        List<String> indexes = waitForIndexes(g, "foo:NUMERIC");
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        Assert.assertFalse(indexes.contains("foo:STRING"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "foo").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertFalse(indexes.contains("foo:NUMERIC"));
        Assert.assertFalse(indexes.contains("foo:STRING"));
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "string").next();
        indexes = waitForIndexes(g, "bar:STRING");
        Assert.assertTrue(indexes.contains("bar:STRING"));
        Assert.assertFalse(indexes.contains("bar:NUMERIC"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "bar").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertFalse(indexes.contains("bar:STRING"));
        Assert.assertFalse(indexes.contains("bar:NUMERIC"));
    }

    @Test
    public void testDropAllOnNoExistingSindex() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "foo").
                with("index_type", "numeric").next();
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", "bar").
                with("index_type", "string").next();
        List<String> indexes = waitForIndexes(g, "foo:NUMERIC", "bar:STRING");
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        Assert.assertTrue(indexes.contains("bar:STRING"));
        g.call("aerospike.graph.admin.index.drop").
                with("element_type", "vertex").
                with("property_key", "baz").next();
        indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertTrue(indexes.contains("foo:NUMERIC"));
        Assert.assertTrue(indexes.contains("bar:STRING"));
    }
}
