package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.sql.AnalysisException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.bulkloader.integration.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;

public class TestBulkLoaderCallEntryPoint {
    @Test
    public void invalidNoVerticesNoEdges() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("edges", false).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'."));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "invalid path").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e instanceof AnalysisException);
                Assert.assertTrue(e.getMessage().startsWith("[PATH_NOT_FOUND] Path does not exist:"));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", null).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("configure", "path").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.sampling-percentage", "invalid").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.sampling-percentage", true).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.keep-provided-edge-id-as-property", "boolean").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.keep-provided-edge-id-as-property", 123).iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", true).with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices").with("edges").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", true).with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void validateInputDataTrue() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("validate_input_data", true).iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void validateInputDataFalse() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("validate_input_data", false).iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void validateInputDataInvalidInput() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("validate_input_data", "notABoolean").iterate();
            Assert.fail("Expected call to fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("Illegal arguments provided to 'aerospike.graphloader.admin.bulk-load.load'"));
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.sampling-percentage", 50).iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.sampling-percentage", "50").iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.keep-provided-edge-id-as-property", true).iterate();
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
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").with("aerospike.graphloader.keep-provided-edge-id-as-property", "true").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void concurrentBulkLoad() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final Thread existingLoad = new Thread(() -> g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate());
            existingLoad.start();
            Thread.sleep(500);
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
                Assert.fail("Starting a bulk load when one was already running did not fail when it should have.");
            } catch (final RuntimeException e) {
                Assert.assertEquals(JOB_ALREADY_RUNNING, e.getMessage());
            }
            existingLoad.join();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());

            g.V().drop().iterate();
            Thread.sleep(5000);
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Ignore("TODO GRAPH-888: NPE caused by org.codehaus.groovy.reflection.ReflectionUtils.VM_PLUGIN is null on CI machine")
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
            g.call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties")
                    .with("aerospike.graphloader.vertices", "s3://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "s3://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("AWS_ACCESS_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("AWS_SECRET_ACCESS_KEY"))
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
            g.call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties")
                    .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices/")
                    .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                    .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                    .iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }
    @Test
    public void testIncrementalLoad() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            Assert.assertEquals(12L, g.V().count().next().longValue());
            Assert.assertEquals(23L, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            Assert.assertEquals(12L, g.V().count().next().longValue());
            Assert.assertEquals(23L, g.E().count().next().longValue());
            // assert simon doesnt drive f150
            // assert property isDope not present
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-2.properties").iterate();
            Assert.assertEquals(13L, g.V().count().next().longValue());
            Assert.assertEquals(24L, g.E().count().next().longValue());
            // assert simon does drive f150
            // assert property isDope added
        }
    }

    @Test
    public void readData() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            List<Map<Object, Object>> vertexIds = g.V().elementMap().toList();
            List<Map<Object, Object>> edgeIds = g.E().elementMap().toList();
            System.out.println("edge id count: " + g.E().count().next());
            System.out.println("Vertex IDs: " + vertexIds);
            System.out.println("Edge IDs: " + edgeIds);
        }
    }
}
