package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDetectSupernodes;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateVerifyEdges;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateVerifyVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteEdges;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteVertices;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoadFail;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestSparkStateMachineRecovery {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config-recovery.properties";
    private final String FAIL_EDGE_WRITE = "src/test/resources/conf/packed/config-recovery-fail-edge-write.properties";
    private final String FAIL_EDGE_VERIFY = "src/test/resources/conf/packed/config-recovery-fail-edge-verify.properties";
    private final String FAIL_VERTEX_WRITE = "src/test/resources/conf/packed/config-recovery-fail-vertex-write.properties";
    private final String FAIL_VERTEX_VERIFY = "src/test/resources/conf/packed/config-recovery-fail-vertex-verify.properties";
    private final String FAIL_SUPERNODE = "src/test/resources/conf/packed/config-recovery-fail-supernodes.properties";
    private final String SAMPLE_SUPERNODE = "src/test/resources/conf/packed/config-sampling-supernodes-vertex-fail.properties";

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
        RecoveryUtil.truncate(graph);
    }

    @BeforeClass
    public static void generateData() throws IOException, InterruptedException, ExecutionException {
        final Process python = Runtime.getRuntime().exec("python src/test/resources/csv-generate.py");
        if (python.onExit().get().exitValue() != 0) {
            throw new RuntimeException("Failed to generate csv data to run tests.");
        }
    }

    @AfterClass
    public static void clearData() throws IOException {
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/vertices/vertexList"));
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/edges/edgeList"));
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    @Test
    public void testSupernodeDetectionFailure() {
        System.out.println("Testing testSupernodeDetectionFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_SUPERNODE}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should not have loaded supernodes.
        Assert.assertTrue(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.edgeDataset);

        // Should not have loaded any partitions.
        Assert.assertTrue(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());

        // Expect default start state to be read vertices.
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateDetectSupernodes);
    }

    @Test
    public void testForceFlag() {
        System.out.println("Testing testForceFlag");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_SUPERNODE}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Should have thrown an exception");
        } catch (IllegalStateException exception) {
            Assert.assertTrue(exception.getMessage().contains("Bulk load resume information is present."));
        }

        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-force"});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();
    }

    @Test
    public void testSampling() {
        System.out.println("Testing testSampling");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", SAMPLE_SUPERNODE}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());
    }

    @Test
    public void testVertexWritingFailure() {
        System.out.println("Testing testVertexWritingFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_VERTEX_WRITE}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.edgeDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateWriteVertices);
    }

    @Test
    public void testVertexVerificationFailure() {
        System.out.println("Testing testVertexVerificationFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_VERTEX_VERIFY}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.edgeDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateVerifyVertices);
    }

    @Test
    public void testEdgeWritingFailure() {
        System.out.println("Testing testEdgeWritingFailure");

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_WRITE}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also, should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.edgeDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertFalse(stateMachine.completedEdgePartitions.contains(3L));
        Assert.assertFalse(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateWriteEdges);
    }

    @Test
    public void testEdgeVerificationFailure() {
        System.out.println("Testing testEdgeVerificationFailure");

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_VERIFY}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should have loaded supernodes. Also, should have loaded vertex and edge dataset.
        Assert.assertFalse(stateMachine.supernodes.isEmpty());

        // Should have loaded vertex and edge dataset.
        Assert.assertNotNull(stateMachine.vertexDataset);
        Assert.assertNotNull(stateMachine.edgeDataset);

        // Should have loaded any vertex partitions but no edge partitions.
        Assert.assertFalse(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertFalse(stateMachine.completedEdgePartitions.isEmpty());
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateVerifyEdges);
    }
}
