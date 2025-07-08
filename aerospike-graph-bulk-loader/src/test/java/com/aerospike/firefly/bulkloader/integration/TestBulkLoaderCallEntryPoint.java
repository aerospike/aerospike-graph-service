package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.integration.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_EXCEPTION_MESSAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_ERROR;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.PROGRESS_COMPLETE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.structure.FireflyGraph.BULK_LOAD_VERTEX_ADD_KEY;

public class TestBulkLoaderCallEntryPoint {
    private final Configuration config;

    public TestBulkLoaderCallEntryPoint() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Before
    public void beforeEach() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            RecoveryUtil.truncate(fireflyGraph);
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
            final var g = fireflyGraph.traversal();
            g.V().drop().iterate();
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with("vertices").
                    with("edges").
                    with("clear_existing_data").
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
            while (!(boolean)status.get(PROGRESS_COMPLETE)) {
                status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
            }
            Assert.assertEquals(status.get(BULK_LOAD_STATUS_KEY), BULK_LOAD_STATUS_ERROR);
            Assert.assertTrue(((String)status.get(BULK_LOAD_EXCEPTION_MESSAGE)).contains("Cannot clear existing data when database is empty and no recovery information is present."));
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
                Assert.assertEquals("Error, failed to find the configuration file at 'invalid path'.", e.getMessage());
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
                Assert.assertEquals("Error, 'aerospike.graphloader.config' cannot be null, but null was passed in.", e.getMessage());
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
            waitForBulkLoad(g);
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            waitForBulkLoad(g);
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
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", true).with("edges", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            waitForBulkLoad(g);
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").with("vertices", false).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            final long vertexCount = g.V().count().next().longValue();
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").with("edges", true).with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
            existingLoad.join();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
                Assert.fail("Starting a bulk load when one was already running did not fail when it should have.");
            } catch (final RuntimeException e) {
                Assert.assertEquals(JOB_ALREADY_RUNNING, e.getMessage());
            }
            waitForBulkLoad(g);
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());

            g.V().drop().iterate();
            Thread.sleep(5000);
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties").iterate();
            waitForBulkLoad(g);
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
            waitForBulkLoad(g);
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
                    .with("aerospike.graphloader.vertices", "gs://gha-ci-firefly-bulkloader/vertices_ags3/")
                    .with("aerospike.graphloader.edges", "gs://gha-ci-firefly-bulkloader/edges_ags3/")
                    .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                    .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                    .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                    .iterate();
            waitForBulkLoad(g);
            Assert.assertNotEquals(0, g.V().count().next().longValue());
            Assert.assertNotEquals(0, g.E().count().next().longValue());
        }
    }

    @Test
    public void testDifferentConfigFile() {
        final Configuration differentConfig = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        differentConfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID, "somethingdifferent");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(differentConfig)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            try {
                g.call("aerospike.graphloader.admin.bulk-load.load").
                        with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            } catch (final IllegalStateException e) {
                Assert.assertEquals("Error, attempting to load graph id '0' through call step on graph id 'somethingdifferent'.", e.getMessage());
            }
        }
    }

    @Test
    public void testIncrementalLoad() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            waitForBulkLoad(g);
            Assert.assertEquals(12L, g.V().count().next().longValue());
            Assert.assertEquals(23L, g.E().count().next().longValue());
            List<Map<Object, Object>> lyndon1 = g.V().has("name", "Lyndon").elementMap().toList();
            Assert.assertEquals(1, lyndon1.size());
            List<? extends Property> lyndonCompanies = g.V().has("name", "Lyndon").properties("companies").toList();
            Assert.assertEquals(2, lyndonCompanies.size());
            Assert.assertEquals(Set.of("Apache TinkerPop", "Aerospike"),
                    lyndonCompanies.stream().map(Property::value).collect(Collectors.toSet()));
            List<Object> simonDrives1 = g.V("simon").out("drives").id().toList();
            Assert.assertEquals(1, simonDrives1.size());
            Assert.assertEquals("GR86", simonDrives1.get(0));

            // Identical vertex merge.
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-1.properties").iterate();
            waitForBulkLoad(g);
            Assert.assertEquals(12L, g.V().count().next().longValue());
            Assert.assertEquals(46L, g.E().count().next().longValue());

            List<Object> simonDrives2 = g.V("simon").out("drives").id().toList();
            Assert.assertEquals(2, simonDrives2.size());
            Assert.assertEquals(Set.of("GR86"), ((List) simonDrives2).stream().collect(Collectors.toSet()));

            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-2.properties").iterate();
            waitForBulkLoad(g);
            Assert.assertEquals(13L, g.V().count().next().longValue());
            Assert.assertEquals(70L, g.E().count().next().longValue());
            Assert.assertTrue(g.V().has("name", "simon").properties("isDope").toList().isEmpty());

            List<Object> simonDrives3 = g.V("simon").out("drives").id().toList();
            Assert.assertEquals(4, simonDrives3.size());
            Assert.assertEquals(Set.of("GR86", "f150"), ((List) simonDrives3).stream().collect(Collectors.toSet()));
            final List<? extends Property> lyndon2 = g.V().has("name", "Lyndon1").properties("companies").toList();
            Assert.assertEquals(6, lyndon2.size());
            int apacheTinkerPopCount = 0;
            int apacheTinkerPop1Count = 0;
            int aerospikeCount = 0;
            for (final Property p : lyndon2) {
                if ("Apache TinkerPop".equals(p.value())) {
                    apacheTinkerPopCount++;
                } else if ("Apache TinkerPop1".equals(p.value())) {
                    apacheTinkerPop1Count++;
                } else if ("Aerospike".equals(p.value())) {
                    aerospikeCount++;
                }
            }
            Assert.assertEquals(2, apacheTinkerPopCount);
            Assert.assertEquals(1, apacheTinkerPop1Count);
            Assert.assertEquals(3, aerospikeCount);
            Assert.assertTrue(g.V().has("name", "Lyndon").toList().isEmpty());
            g.V("simon").properties("isDope").toList().forEach(p -> Assert.assertEquals("true", p.value()));
            Assert.assertFalse(g.V().has(BULK_LOAD_VERTEX_ADD_KEY).hasNext());
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
            waitForBulkLoad(g);
            FireflyVertex supernode = (FireflyVertex) g.V("supernode").next();
            Key supernodeKey = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode.id);
            Record r = fireflyGraph.getBaseGraph().read(supernodeKey, null);
            Map<String, List<Object>> edgeCache = (Map) r.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(1, edgeCache.size());
            Assert.assertEquals(1, edgeCache.get("edge").size());

            // Only change we expect is that now this vertex is a supernode.
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config",
                            "src/test/resources/conf/packed/config-incremental-new-supernode-incremental.properties").iterate();
            waitForBulkLoad(g);
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
    public void testMultiProperties() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            // ~id,~label,test_multi_before_not_after:string(list),test_multi_before_and_after:string(list),test_multi_not_before_but_after:string,test_multi_not_before_not_after:string
            // 1  ,person,before;not;after                        ,before;and;after                        ,not;before;but;after                  ,not;before;not;after

            // ~label,~from,~to,test(list)
            // knows,1,1,foo;bar
            g.call("aerospike.graphloader.admin.bulk-load.load")
                .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-multi-1.properties")
                .iterate();
            waitForBulkLoad(g);
            Assert.assertEquals(1, g.V().count().next().longValue());
            Assert.assertEquals(1, g.E().count().next().longValue());
            final FireflyVertex vertex = (FireflyVertex) g.V().next();
            testProperty(vertex, "test_multi_before_not_after", List.of("before", "not", "after"));
            testProperty(vertex, "test_multi_before_and_after", List.of("before", "and", "after"));
            testProperty(vertex, "test_multi_not_before_but_after", List.of("not;before;but;after"));
            testProperty(vertex, "test_multi_not_before_not_after", List.of("not;before;not;after"));
            testProperty(fireflyGraph, vertex.id(), "test_multi_before_not_after", List.of("before", "not", "after"));
            testProperty(fireflyGraph, vertex.id(), "test_multi_before_and_after", List.of("before", "and", "after"));
            testProperty(fireflyGraph, vertex.id(), "test_multi_not_before_but_after", List.of("not;before;but;after"));
            testProperty(fireflyGraph, vertex.id(), "test_multi_not_before_not_after", List.of("not;before;not;after"));
            final FireflyEdge edge = (FireflyEdge) g.V().outE("knows").next();
            testProperty(edge, "test", Set.of("foo", "bar"));

            // Incremental load next dataset.
            // ~id,~label,test_multi_before_not_after:string,test_multi_before_and_after:string(list),test_multi_not_before_but_after:string(list),test_multi_not_before_not_after:string
            // 1  ,person,present                           ,before;baz                              ,not;before;but;after                        ,still;not
            //
            // ~label,~from,~to,test(list)
            // knows,1,1,foo;bar
            g.call("aerospike.graphloader.admin.bulk-load.load")
                .with(INCREMENTAL_LOAD, true)
                .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config-incremental-multi-2.properties")
                .iterate();
            waitForBulkLoad(g);
            Assert.assertEquals(1, g.V().count().next().longValue());
            Assert.assertEquals(2, g.E().count().next().longValue());
            final FireflyVertex vertex2 = (FireflyVertex) g.V().next();
            testProperty(vertex2, "test_multi_before_not_after", List.of("present"));
            testProperty(vertex2, "test_multi_before_and_after", List.of("before", "and", "after", "before", "baz"));
            testProperty(vertex2, "test_multi_not_before_but_after", List.of("not;before;but;after", "not", "before", "but", "after"));
            testProperty(vertex2, "test_multi_not_before_not_after", List.of("still;not"));
            testProperty(fireflyGraph, vertex2.id(), "test_multi_before_not_after", List.of("present"));
            testProperty(fireflyGraph, vertex2.id(), "test_multi_before_and_after", List.of("before", "and", "after", "before", "baz"));
            testProperty(fireflyGraph, vertex2.id(), "test_multi_not_before_but_after", List.of("not;before;but;after", "not", "before", "but", "after"));
            testProperty(fireflyGraph, vertex2.id(), "test_multi_not_before_not_after", List.of("still;not"));
        }
    }

    void testProperty(final FireflyEdge edge, final String property, final Set<String> expectedValues) {
        final Property<Object> edgeProperty = edge.property(property);
        final List<Object> propertyValue = (List<Object>) edgeProperty.value();
        Assert.assertEquals(expectedValues, new HashSet<>(propertyValue));
    }

    void testProperty(final FireflyVertex vertex, final String property, final List<String> expectedValues) {
        final Iterator<VertexProperty<Object>> propertyIterator = vertex.properties(property);
        final List<String> propertyList = new ArrayList<>();
        propertyIterator.forEachRemaining(vp -> propertyList.add((String) vp.value()));
        Assert.assertEquals(expectedValues.size(), propertyList.size());
        Assert.assertEquals(new HashSet<>(expectedValues), new HashSet<>(propertyList));
    }

    void testProperty(final FireflyGraph graph, final Object id, final String property, final List<String> expectedValues) {
        final List<? extends Property<Object>> propertyList = graph.traversal().V(id).properties(property).toList();
        final List<String> propertyValues = propertyList.stream().map(Property::value).map(Object::toString).collect(Collectors.toList());
        Assert.assertEquals(expectedValues.size(), propertyValues.size());
        Assert.assertEquals(new HashSet<>(expectedValues), new HashSet<>(propertyValues));
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
            waitForBulkLoad(g);
            FireflyVertex supernode = (FireflyVertex) g.V("supernode").next();
            Key supernodeKey = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode.id);
            Record r = fireflyGraph.getBaseGraph().read(supernodeKey, null);
            Map<String, List<Object>> edgeCache = (Map) r.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(0, edgeCache.size());
            Assert.assertTrue(g.V("supernode").out().count().next() > fireflyGraph.getBaseGraph().ON_RECORD_ID_LIMIT);
            final Long supernodeOutCount = g.V("supernode").out().count().next();

            // Only change we expect is that there is now an additional edge on the supernode.
            RecoveryUtil.truncate(fireflyGraph);
            g.call("aerospike.graphloader.admin.bulk-load.load").
                    with(INCREMENTAL_LOAD, true).
                    with("aerospike.graphloader.config",
                            "src/test/resources/conf/packed/config-incremental-old-supernode-incremental.properties").iterate();
            waitForBulkLoad(g);
            FireflyVertex supernode2 = (FireflyVertex) g.V("supernode").next();
            Key supernodeKey2 = getKey(fireflyGraph.getBaseGraph(), fireflyGraph.getBaseGraph().VERTEX_AERO_SET, supernode2.id);
            Record r2 = fireflyGraph.getBaseGraph().read(supernodeKey2, null);
            Map<String, List<Object>> edgeCache2 = (Map) r2.getMap(fireflyGraph.getBaseGraph().OUT_EDGES_BIN);
            Assert.assertEquals(0, edgeCache2.size());
            Assert.assertEquals(supernodeOutCount + 1, g.V("supernode").out().count().next().longValue());
        }
    }

    @Test
    public void test62mCsvOnGcs() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            Assert.assertEquals(0, g.V().count().next().longValue());
            Assert.assertEquals(0, g.E().count().next().longValue());
            g.call("aerospike.graphloader.admin.bulk-load.load")
                .with("aerospike.graphloader.config", "src/test/resources/conf/packed/config.properties")
                .with("aerospike.graphloader.vertices", "gs://incremental-datasets/synthetic/62m/vertices/")
                .with("aerospike.graphloader.edges", "gs://incremental-datasets/synthetic/62m/edges/")
                .with("aerospike.graphloader.remote-user", System.getenv("GCS_PRIVATE_KEY_ID"))
                .with("aerospike.graphloader.remote-passkey", System.getenv("GCS_PRIVATE_KEY"))
                .with("aerospike.graphloader.gcs-email", System.getenv("GCS_CLIENT_EMAIL"))
                .next();
            waitForBulkLoad(g);
        }
    }
}
