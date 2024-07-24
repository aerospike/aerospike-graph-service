package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDetectSupernodes;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateReadVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateVerifyEdges;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateVerifyVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteEdges;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteVertices;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestBulkLoaderRecovery {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config-recovery.properties";

    private Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    private String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }
    private static final String[] DEFAULT_PARAMS= {"-validate_input_data", "-verify_output_data"};
    protected FireflyGraph graph = null;

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
        System.clearProperty("bulkloader.testing.partition.failure.supernode");
        System.clearProperty("bulkloader.testing.partition.failure.vertex.writing");
        System.clearProperty("bulkloader.testing.partition.failure.vertex.verification");
        System.clearProperty("bulkloader.testing.partition.failure.edge.writing");
        System.clearProperty("bulkloader.testing.partition.failure.edge.verification");
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    @Test
    public void testSupernodeDetectionFailure() {
        System.setProperty("bulkloader.testing.partition.failure.supernode", "true");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should not have loaded supernodes.
        Assert.assertTrue(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.persistedEdgeIdDataset);

        // Should not have loaded any partitions.
        Assert.assertTrue(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());

        // Expect default start state to be read vertices.
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateDetectSupernodes);

    }

    @Test
    public void testVertexWritingFailure() {
        System.setProperty("bulkloader.testing.partition.failure.vertex.writing", "3");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            // Expected
        }
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();
        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.persistedEdgeIdDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateWriteVertices);
    }

    @Test
    public void testVertexVerificationFailure() {
        System.setProperty("bulkloader.testing.partition.failure.vertex.verification", "true");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            // Expected
        }
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.persistedEdgeIdDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateVerifyVertices);
    }

    @Test
    public void testEdgeWritingFailure() {
        System.setProperty("bulkloader.testing.partition.failure.edge.writing", "3");

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            // Expected
        }
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.persistedEdgeIdDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertFalse(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateWriteEdges);
    }

    @Test
    public void testEdgeVerificationFailure() {
        System.setProperty("bulkloader.testing.partition.failure.edge.verification", "true");

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            // Expected
        }
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.persistedEdgeIdDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertFalse(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateVerifyEdges);
    }

    @Test
    public void checkpoint() {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("CheckpointCSVExample")
                .config("spark.master", "local")  // Run locally for simplicity
                .getOrCreate();

        // Define the path to your CSV file
        String csvFilePath = "/home/lyndon/github/firefly/aerospike-graph-bulk-loader/src/test/resources/recoverydata/edges"; // Replace with your actual CSV file path

        // Read the CSV file with headers
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv(csvFilePath);

        // Define the checkpoint directory
        String checkpointDir = "/home/lyndon/github/firefly/aerospike-graph-bulk-loader/src/test/resources/recoverydata/checkpoint"; // Replace with your checkpoint directory

        // Set the checkpoint directory
        spark.sparkContext().setCheckpointDir(checkpointDir);

        // Checkpoint the DataFrame
        df.checkpoint();

        // Save the checkpointed DataFrame to a file (optional)
        //df.write().mode("overwrite").parquet(checkpointDir);

        Dataset<Row> rows = spark.read().parquet(checkpointDir);

        // Stop the Spark session
        spark.stop();
    }

    // Test cases:
    // 1. Failure before supernode detection
    // 2. Failure during vertex writing
    // 3. Failure during vertex verification (should not include verification issues)
    // 4. Failure during edge writing
    // 5. Failure during edge verification (should not include verification issues)
}
