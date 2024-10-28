package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.integration.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;

public class TestBulkLoaderCallEntryPoint {
    private final Configuration config;

    public TestBulkLoaderCallEntryPoint() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_DISABLED.toLowerCase(), "true");
    }

    @Before
    public void beforeEach() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
        }
    }

    @Test
    public void invalidNoVerticesNoEdges() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
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
    public void testClearExistingDataDoesntThrow() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().V().drop().iterate();
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").
                        with("vertices").
                        with("edges").
                        with("clear_existing_data").
                        with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("Cannot clear existing data when database is empty and no recovery information is present."));
            }
        }
    }

    @Test
    public void invalidConfigPath() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            try {
                fireflyGraph.traversal().call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "invalid path").iterate();
                Assert.fail("Expected call to fail.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("[PATH_NOT_FOUND] Path does not exist:"));
            }
        }
    }

    @Test
    public void invalidConfigPathNull() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
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
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithBooleans() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", true).with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithFalseOnly() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeSeparatelyWithTrueOnly() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Assert.assertEquals(vertexCount, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void vertexEdgeTogetherWith() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
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
    public void testEmptyPath() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").
                        with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-empty.properties").iterate();
                Assert.fail("Expected exception to be thrown.");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("Failed to read directories from src/test/resources/sampledata-empty/vertices. " +
                        "This is usually the result of an empty directory or missing headers. Look at the directory and ensure it is populated with valid csv files."));
            }
        }
    }

    @Test
    public void numericConfigAsString() {
        // Right now calling the bulk loader here will fail with null config.
        // Once the parameters are determined this test can be updated.
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
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
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
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            Assert.assertEquals(12L, g.V().count().next().longValue());
            Assert.assertEquals(23L, g.E().count().next().longValue());
            List<Map<Object, Object>> lyndon1 = g.V().has("name", "Lyndon").elementMap().toList();
            Assert.assertEquals(1, lyndon1.size());
            Assert.assertEquals(Set.of("Apache TinkerPop", "Aerospike"),
                    ((List) lyndon1.get(0).get("companies")).stream().collect(Collectors.toSet()));
            List<Object> simonDrives1 = g.V("simon").out("drives").id().toList();
            Assert.assertEquals(1, simonDrives1.size());
            Assert.assertEquals("GR86", simonDrives1.get(0));

            // Identical vertex merge.
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            Assert.assertEquals(12L, g.V().count().next().longValue());
            Assert.assertEquals(46L, g.E().count().next().longValue());

            List<Object> simonDrives2 = g.V("simon").out("drives").id().toList();
            Assert.assertEquals(2, simonDrives2.size());
            Assert.assertEquals(Set.of("GR86"), ((List) simonDrives2).stream().collect(Collectors.toSet()));

            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-2.properties").iterate();
            Assert.assertEquals(13L, g.V().count().next().longValue());
            Assert.assertEquals(70L, g.E().count().next().longValue());
            Assert.assertTrue(g.V().has("name", "simon").properties("isDope").toList().isEmpty());

            List<Object> simonDrives3 = g.V("simon").out("drives").id().toList();
            Assert.assertEquals(4, simonDrives3.size());
            Assert.assertEquals(Set.of("GR86", "f150"), ((List) simonDrives3).stream().collect(Collectors.toSet()));
            List<Map<Object, Object>> lyndon2 = g.V().has("name", "Lyndon1").elementMap().toList();
            Assert.assertEquals(1, lyndon2.size());
            Assert.assertEquals(Set.of("Apache TinkerPop1", "Aerospike"),
                    ((List) lyndon2.get(0).get("companies")).stream().collect(Collectors.toSet()));
            Assert.assertTrue(g.V().has("name", "Lyndon").toList().isEmpty());
            g.V("simon").properties("isDope").toList().forEach(p -> Assert.assertEquals("true", p.value()));
        }
    }

    @Test
    public void testLoadWithCsvFileDirectlyFails() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").
                        with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-direct-csv-vertices.properties").iterate();
                Assert.fail("Expected call to fail with direct csv of vertices.");
            } catch (final RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Config aerospike.graphloader.vertices must be a directory and cannot be a single file, current value is src/test/resources/sampledata-incremental-1/vertices.csv"));
            }
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").
                        with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-direct-csv-edges.properties").iterate();
                Assert.fail("Expected call to fail with direct csv of edges.");
            } catch (final RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Config aerospike.graphloader.edges must be a directory and cannot be a single file, current value is src/test/resources/sampledata-incremental-1/edges.csv"));
            }
        }
    }

    @Test
    public void testIncrementalLoadNewSupernode() {
        // This test will test a dataset where there was previously not a supernode and we are adding to it such that it will
        // become a supernode from the combination of the previous cache plus the new data.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertTrue(g.V("supernode").toList().isEmpty());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with("aerospike.graphloader.config",
                            "src/test/resources/conf/packed/config-incremental-new-supernode-initial.properties").iterate();
            FireflyVertex supernode = (FireflyVertex) g.V("supernode").next();
            Key supernodeKey = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode.id);
            Record r = fireflyGraph.getBaseGraph().read(supernodeKey, null);
            Map<String, List<Object>> edgeCache = (Map) r.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(1, edgeCache.size());
            Assert.assertEquals(1, edgeCache.get("edge").size());

            // Only change we expect is that now this vertex is a supernode.
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config",
                            "src/test/resources/conf/packed/config-incremental-new-supernode-incremental.properties").iterate();
            supernode = (FireflyVertex) g.V("supernode").next();
            supernodeKey = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode.id);
            r = fireflyGraph.getBaseGraph().read(supernodeKey, null);
            edgeCache = (Map) r.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(1, edgeCache.size());
            Assert.assertEquals(1, edgeCache.get("edge").size());
            Assert.assertTrue(g.V("supernode").out().count().next() > fireflyGraph.getBaseGraph().ON_RECORD_ID_LIMIT);
        }
    }

    @Test
    public void testIncrementalLoadOldSupernode() {
        // This test will test a dataset where there was previously a supernode and we are adding to it.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertTrue(g.V("supernode").toList().isEmpty());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with("aerospike.graphloader.config",
                            "src/test/resources/conf/packed/config-incremental-old-supernode-initial.properties").iterate();
            FireflyVertex supernode = (FireflyVertex) g.V("supernode").next();
            Key supernodeKey = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode.id);
            Record r = fireflyGraph.getBaseGraph().read(supernodeKey, null);
            Map<String, List<Object>> edgeCache = (Map) r.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(0, edgeCache.size());
            Assert.assertTrue(g.V("supernode").out().count().next() > fireflyGraph.getBaseGraph().ON_RECORD_ID_LIMIT);
            final Long supernodeOutCount = g.V("supernode").out().count().next();

            // Only change we expect is that there is now an additional edge on the supernode.
            RecoveryUtil.truncate(fireflyGraph.getBaseGraph());
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config",
                            "src/test/resources/conf/packed/config-incremental-old-supernode-incremental.properties").iterate();
            FireflyVertex supernode2 = (FireflyVertex) g.V("supernode").next();
            Key supernodeKey2 = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode2.id);
            Record r2 = fireflyGraph.getBaseGraph().read(supernodeKey2, null);
            Map<String, List<Object>> edgeCache2 = (Map) r2.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(0, edgeCache2.size());
            Assert.assertEquals(supernodeOutCount + 1, g.V("supernode").out().count().next().longValue());
        }
    }
	
	@Test
    public void test500mCsvOnGcs() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            Assert.assertEquals("Success", g.with("evaluationTimeout", 120 * 60 * 1000)
                    .call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties")
                    .with("aerospike.graphloader.vertices", "gs://incremental-datasets/synthetic/500m/vertices/")
                    .with("aerospike.graphloader.edges", "gs://incremental-datasets/synthetic/500m/edges/")
                    .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                    .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                    .next());
        }
    }
}
