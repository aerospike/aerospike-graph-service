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
    public void invalidConfigPath() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.config", "invalid path").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("[PATH_NOT_FOUND]"));
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
                fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.config", null).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'aerospike.graphloader.config' to be set to a String value. Instead value was null.", e.getMessage());
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
                Assert.assertTrue(e.getMessage().startsWith("The bulk loader allows the following parameters: "));
                Assert.assertTrue(e.getMessage().endsWith(". The following provided parameters are not allowed: [configure]."));
            }
        }
    }

    @Test
    public void invalidNumericStringKey() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.sampling-percentage", "invalid").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'aerospike.graphloader.sampling-percentage' to be set to a numeric value or numeric String value. Instead value was set with type 'java.lang.String' which cannot be parsed to a numeric value.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidNumericKey() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.sampling-percentage", true).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'aerospike.graphloader.sampling-percentage' to be set to a numeric value or numeric String value. Instead value was set with type 'java.lang.Boolean'.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidBooleanStringKey() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.keep-provided-edge-id-as-property", "boolean").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'aerospike.graphloader.keep-provided-edge-id-as-property' to be set to a boolean value or boolean String value. Instead value was set with type 'java.lang.String' which cannot be parsed to a boolean value.", e.getMessage());
            }
        }
    }

    @Test
    public void invalidBooleanKey() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.keep-provided-edge-id-as-property", 123).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertEquals("Expected bulk loader flag 'aerospike.graphloader.keep-provided-edge-id-as-property' to be set to a boolean value or boolean String value. Instead value was set with type 'java.lang.Integer'.", e.getMessage());
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
            g.call("bulk-load").with("vertices").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("edges").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("bulk-load").with("vertices", true).with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("vertices", false).with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("bulk-load").with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("vertices", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("bulk-load").with("vertices", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("bulk-load").with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("bulk-load").with("vertices").with("edges").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("bulk-load").with("vertices", true).with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void dryrunTrue() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("dryrun", true).iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void dryrunFalse() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("dryrun", false).iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void dryrunInvalidInput() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("dryrun", "notABoolean").iterate();
            Assert.fail("Expected call to fail.");
        } catch (final Exception e) {
            Assert.assertEquals("Expected bulk loader flag 'dryrun' to be set to a boolean value. Instead value was set with type 'java.lang.String'.", e.getMessage());
        }
    }

    @Test
    public void numericConfig() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.sampling-percentage", 50).iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void numericConfigAsString() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.sampling-percentage", "50").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void booleanConfig() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.keep-provided-edge-id-as-property", true).iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void booleanConfigAsString() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.keep-provided-edge-id-as-property", "true").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void testCsvOnS3() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load")
                    .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties")
                    .with("aerospike.graphloader.vertices", "s3://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "s3://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote.user", System.getenv("AWS_ACCESS_KEY_ID"))
                    .with("aerospike.graphloader.remote.passkey", System.getenv("AWS_SECRET_ACCESS_KEY"))
                    .iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void testCsvOnGcs() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("bulk-load")
                    .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties")
                    .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote.user", System.getenv("GCS_PRIVATE_KEY_ID"))
                    .with("aerospike.graphloader.remote.passkey", System.getenv("GCS_PRIVATE_KEY"))
                    .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                    .iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }
}
