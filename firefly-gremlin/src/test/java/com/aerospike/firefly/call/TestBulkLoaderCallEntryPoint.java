package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestBulkLoaderCallEntryPoint {

    @Test
    public void invalidNoParameters() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Bulk load config path ('config') must be set to a non-empty String value.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidNoVerticesNoEdges() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("vertices", false).with("edges", false).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Either 'vertices' or 'edges' must be set to true.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidAwsConfig() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("config", "src/test/resources/conf/packed/config.properties").with("aws").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Failed to start bulk loader due to null s3BucketName (null)", e.getMessage());
            }
        }
    }

    @Test
    public void invalidNumericConfig() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("config", 1).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'config' to be set to a String value. Instead value was set with type 'java.lang.Integer'.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidConfigPath() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("config", "invalid path").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("java.nio.file.NoSuchFileException: invalid path", e.getMessage());
            }
        }
    }

    @Test
    public void invalidConfigPathEmpty() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("config", "").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Bulk load config path ('config') must be set to a non-empty String value.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidConfigPathNull() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("config", null).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'config' to be set to a String value. Instead value was null.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidConfigKey() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("configure", "path").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("The bulk loader expects the following parameters: "));
                Assert.assertTrue(e.getMessage().endsWith(". The following provided parameters are not allowed: [configure]."));
            }
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithoutBooleans() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("vertices").with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("edges").with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithBooleans() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("vertices", true).with("edges", false).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("vertices", false).with("edges", true).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithFalseOnly() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("edges", false).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("vertices", false).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithTrueOnly() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("vertices", true).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("edges", true).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeTogetherWith() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("vertices").with("edges").with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeTogetherWithTrue() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("vertices", true).with("edges", true).with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeTogetherDefault() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }
}
