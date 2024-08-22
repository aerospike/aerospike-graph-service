package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDetectSupernodes;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateReadVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
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

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.CLEAR_EXISTING_DATA_EMPTY_DATABASE;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DATABASE_NOT_EMPTY;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.INCREMENTAL_AND_CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.INCREMENTAL_AND_RECOVERY_INFO_NO_RESUME_FLAG;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.INCREMENTAL_LOAD_EMPTY_DATABASE;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.RECOVERY_INFO_NO_CLEAR_EXISTING_DATA_FLAG_OR_RESUME;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.RESUME_AND_CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.RESUME_WITHOUT_RECOVERY_INFO;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestRecoveryActionFlags {
    // Flags in question:
    // resume, incremental, and clear_existing_data

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config-recovery-prepopulated.properties";

    private Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    private String getDefaultConfig() {
        RecoveryUtil.writeTempDirectory(graph.getBaseGraph(), "/actions-runner/_work/firefly/firefly/aerospike-graph-bulk-loader/src/test/resources/recoverydata/testing-recovery/recovery/edge/");
        return DEFAULT_CONFIG;
    }

    protected FireflyGraph graph = null;

    @BeforeClass
    public static void generateData() throws IOException, InterruptedException, ExecutionException {
        final Process python = Runtime.getRuntime().exec("python3 src/test/resources/csv-generate.py");
        if (python.onExit().get().exitValue() != 0) {
            throw new RuntimeException("Failed to generate csv data to run tests.");
        }
    }

    @AfterClass
    public static void clearData() throws IOException {
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/vertices/vertexList"));
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/edges/edgeList"));
    }

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
        graph.traversal().V().drop().iterate();
        RecoveryUtil.truncate(graph.getBaseGraph());
    }

    @After
    public void afterEach() {
        graph.close();
    }

    public void setPrepInitial() {
        graph.traversal().V().drop().iterate();
    }

    public void setPartiallyWritten() {
        // Partially written means there is data and recovery info available.
        for (int i = 0; i < 300; i++) {
            graph.traversal().addV("test").property("test", "test").next();
        }
        RecoveryUtil.updateState(graph.getBaseGraph(), RecoveryUtil.RecoveryState.DETECT_SUPERNODES);
        RecoveryUtil.updateVertexRecovery(graph.getBaseGraph(), 1);
        RecoveryUtil.updateEdgeRecovery(graph.getBaseGraph(), 1);
    }

    public void setComplete() {
        // Add vertex - complete means data loaded.
        for (int i = 0; i < 300; i++) {
            graph.traversal().addV("test").property("test", "test").next();
        }
        // Truncate recovery info, in complete state there is none available.
        RecoveryUtil.truncate(graph.getBaseGraph());
    }

    @Test
    public void testIncrementalResumeFlag() {
        setPrepInitial();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(INCREMENTAL_LOAD_EMPTY_DATABASE));
        }

        setPartiallyWritten();
        SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD});
        SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
        statePartial.executeState();
        Assert.assertTrue(statePartial.transitionState() instanceof SparkBulkLoaderStateDetectSupernodes);

        setComplete();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(RESUME_WITHOUT_RECOVERY_INFO));
        }
    }

    @Test
    public void testIncrementalFlag() {
        setPrepInitial();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(INCREMENTAL_LOAD_EMPTY_DATABASE));
        }

        setPartiallyWritten();
        try {
            SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
            statePartial.executeState();
            Assert.fail("Expected exception when resuming without resume flag.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(INCREMENTAL_AND_RECOVERY_INFO_NO_RESUME_FLAG));
        }

        setComplete();
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                new String[]{"-local", "-c", getDefaultConfig(), "-" + INCREMENTAL_LOAD});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();
    }

    @Test
    public void testResumeFlag() {
        setPrepInitial();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(RESUME_WITHOUT_RECOVERY_INFO));
        }

        setPartiallyWritten();
        SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
        SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
        statePartial.executeState();
        Assert.assertTrue(statePartial.transitionState() instanceof SparkBulkLoaderStateDetectSupernodes);

        setComplete();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(RESUME_WITHOUT_RECOVERY_INFO));
        }
    }

    @Test
    public void testClearExistingDataFlag() {
        setPrepInitial();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when clear existing data without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(CLEAR_EXISTING_DATA_EMPTY_DATABASE));
        }

        setPartiallyWritten();
        SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA});
        SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
        statePartial.executeState();
        Assert.assertTrue(statePartial.transitionState() instanceof SparkBulkLoaderStateReadVertices);

        setComplete();
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();
        Assert.assertTrue(statePartial.transitionState() instanceof SparkBulkLoaderStateReadVertices);
    }

    @Test
    public void testClearExistingDataIncremental() {
        setPrepInitial();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA, "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(INCREMENTAL_AND_CLEAR_EXISTING_DATA));
        }

        try {
            setPartiallyWritten();
            SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA, "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
            statePartial.executeState();
            Assert.fail("Expected exception when resuming with partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(INCREMENTAL_AND_CLEAR_EXISTING_DATA));
        }

        setComplete();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA, "-" + INCREMENTAL_LOAD});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(INCREMENTAL_AND_CLEAR_EXISTING_DATA));
        }
    }

    @Test
    public void testClearExistingDataResume() {
        setPrepInitial();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA, "-" + RESUME});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(RESUME_AND_CLEAR_EXISTING_DATA));
        }

        try {
            setPartiallyWritten();
            SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA, "-" + RESUME});
            SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
            statePartial.executeState();
            Assert.fail("Expected exception when resuming with partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(RESUME_AND_CLEAR_EXISTING_DATA));
        }

        setComplete();
        try {
            SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig(), "-" + CLEAR_EXISTING_DATA, "-" + RESUME});
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(RESUME_AND_CLEAR_EXISTING_DATA));
        }
    }

    @Test
    public void testNoFlags() {
        setPrepInitial();
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(
                new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        try {
            setPartiallyWritten();
            SparkBulkLoaderStateMachine stateMachinePartial = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig()});
            SparkBulkLoaderState statePartial = new SparkBulkLoaderStateStart(stateMachinePartial);
            statePartial.executeState();
            Assert.fail("Expected exception when resuming with partially written data.");
        } catch (Exception e) {
            System.out.println(RECOVERY_INFO_NO_CLEAR_EXISTING_DATA_FLAG_OR_RESUME);
            Assert.assertTrue(e.getMessage().contains(RECOVERY_INFO_NO_CLEAR_EXISTING_DATA_FLAG_OR_RESUME));
        }

        setComplete();
        try {
            stateMachine = new SparkBulkLoaderStateMachine(
                    new String[]{"-local", "-c", getDefaultConfig()});
            state = new SparkBulkLoaderStateStart(stateMachine);
            state.executeState();
            Assert.fail("Expected exception when resuming without partially written data.");
        } catch (Exception e) {
            System.out.println(DATABASE_NOT_EMPTY);
            Assert.assertTrue(e.getMessage().contains(DATABASE_NOT_EMPTY));
        }
    }
}
