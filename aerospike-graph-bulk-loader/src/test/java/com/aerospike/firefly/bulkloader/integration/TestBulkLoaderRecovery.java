package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateReadVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteVertices;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestBulkLoaderRecovery {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config.properties";

    private Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    private String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    private static final String[] DEFAULT_PARAMS= {"-dryrun", "-writeedge", "-writevertex", "-verifyedge", "-verifyvertex"};
    protected FireflyGraph graph = null;

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    @Test
    public void testSupernodeDetectionFailure() {
        System.setProperty("bulkloader.testing.partition.failure.supernode", "true");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
        SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
        state.executeState();

        // Should not have loaded supernodes.
        Assert.assertTrue(stateMachine.supernodes.isEmpty());

        // Should not have loaded vertex and edge dataset.
        Assert.assertNull(stateMachine.vertexDataset);
        Assert.assertNull(stateMachine.edgeDataset);

        // Should not have loaded any partitions.
        Assert.assertTrue(stateMachine.completedVertexPartitions.isEmpty());
        Assert.assertTrue(stateMachine.completedEdgePartitions.isEmpty());

        // Expect default start state to be read vertices.
        final SparkBulkLoaderState nextState = state.transitionState();
        Assert.assertTrue(nextState instanceof SparkBulkLoaderStateReadVertices);

    }

    @Test
    public void testVertexWritingFailure() {
        System.setProperty("bulkloader.testing.partition.failure.vertex.writing", "true");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(new String[]{"-local", "-c", getDefaultConfig()});
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
        System.setProperty("bulkloader.testing.partition.failure.vertex.verification", "true");
    }

    @Test
    public void testEdgeWritingFailure() {
        System.setProperty("bulkloader.testing.partition.failure.edge.writing", "true");
    }

    @Test
    public void testEdgeVerificationFailure() {
        System.setProperty("bulkloader.testing.partition.failure.edge.verification", "true");
    }

    // Test cases:
    // 1. Failure before supernode detection
    // 2. Failure during vertex writing
    // 3. Failure during vertex verification (should not include verification issues)
    // 4. Failure during edge writing
    // 5. Failure during edge verification (should not include verification issues)
}
