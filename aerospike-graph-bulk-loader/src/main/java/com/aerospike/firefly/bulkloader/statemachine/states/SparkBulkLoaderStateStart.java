package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBulkLoaderStateStart extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateStart.class);
    private SparkBulkLoaderState nextState = null;

    public SparkBulkLoaderStateStart(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    private void loadCheckpointDatasets(final RecoveryUtil.RecoveryInfo info) {
        // Update progress bar.
        sparkBulkLoaderStateMachine.progressBar.setPreflightCheckComplete();
        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();


        // Load the edge dataset and vertex dataset checkpoints.
        sparkBulkLoaderStateMachine.persistedEdgeIdDataset =
                sparkBulkLoaderStateMachine.spark.read().load(sparkBulkLoaderStateMachine.checkpointDirectory);
        sparkBulkLoaderStateMachine.vertexDataset =
                sparkBulkLoaderStateMachine.spark.read().load(sparkBulkLoaderStateMachine.checkpointDirectory);

        // Generation vertex and edge operations.
        sparkBulkLoaderStateMachine.vertexOperations = new VertexOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.vertexDirectories);
        sparkBulkLoaderStateMachine.edgeOperations = new EdgeOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.edgeDirectories);

        // Calculate the number of partitions.
        sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;
        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.persistedEdgeIdDataset.rdd().partitions().length;

        // Load the supernodes and partitions.
        sparkBulkLoaderStateMachine.supernodes = info.getSupernodes();
        sparkBulkLoaderStateMachine.completedVertexPartitions = info.getVertexPartitions();
        sparkBulkLoaderStateMachine.completedEdgePartitions = info.getEdgePartitions();
    }

    @Override
    public void executeState() {
        // Recover from checkpoint.
        if (!sparkBulkLoaderStateMachine.readOnly) {
            final RecoveryUtil.RecoveryInfo info =
                    RecoveryUtil.recover(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());
            final String state = info.getState();
            if (state != null && !state.isEmpty()) {
                loadCheckpointDatasets(info);
                switch (state) {
                    // Preflight ->
                    case "detectSupernodes":
                        LOGGER.info("Recovering from detectSupernodes state");
                        // Here we have completed the preflight and persistentance of edge ids.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoints.
                        nextState = new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
                    case "writeVertices":
                        LOGGER.info("Recovering from writeVertices state");
                        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
                        // Here we have completed the supernode detection and died during vertex writing.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoint.
                        // We can also load the supernodes and vertex partitions.
                        nextState = new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
                        break;
                    case "verifyVertices":
                        LOGGER.info("Recovering from verifyVertices state");
                        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
                        sparkBulkLoaderStateMachine.progressBar.setVertexLoadComplete();
                        // Here we have completed the vertex writing and died during vertex verification.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoint.
                        // We can also load the supernodes.
                        // Vertex partitions are irrelevant, and we can restart the vertex verification step.
                        nextState = new SparkBulkLoaderStateVerifyVertices(sparkBulkLoaderStateMachine);
                        break;
                    case "writeEdges":
                        LOGGER.info("Recovering from writeEdges state");
                        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
                        sparkBulkLoaderStateMachine.progressBar.setVertexLoadComplete();
                        sparkBulkLoaderStateMachine.progressBar.setVertexValidationComplete();
                        // Here we have completed the vertex verification and died during edge writing.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoint.
                        // We can also load the supernodes and edge partitions.
                        // Vertex partitions are irrelevant, and we can restart the edge writing step.
                        nextState = new SparkBulkLoaderStateWriteEdges(sparkBulkLoaderStateMachine);
                        break;
                    case "verifyEdges":
                        LOGGER.info("Recovering from verifyEdges state");
                        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
                        sparkBulkLoaderStateMachine.progressBar.setVertexLoadComplete();
                        sparkBulkLoaderStateMachine.progressBar.setVertexValidationComplete();
                        sparkBulkLoaderStateMachine.progressBar.setEdgeLoadComplete();
                        // Here we have completed the edge writing and died during edge verification.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoint.
                        // We can also load the supernodes.
                        // Partitions are irrelevant, and we can restart the edge verification step.
                        nextState = new SparkBulkLoaderStateVerifyEdges(sparkBulkLoaderStateMachine);
                    default:
                        // Unknown state
                        // This should never happen.
                        throw new IllegalStateException("Error during bulk load recovery, unknown state: " + state);
                }
            } else {
                // Fresh load, truncate any metadata.
                RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());

                // No state found, start from the beginning.
                LOGGER.info("Unable to find state to recover from. Restarting from beginning.");
                nextState = new SparkBulkLoaderStateReadVertices(sparkBulkLoaderStateMachine);
            }
        } else {
            // Fresh load, truncate any metadata.
            RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());

            // Read only mode, start from the beginning.
            LOGGER.info("Read only mode detected. Starting from the beginning.");
            nextState = new SparkBulkLoaderStateReadVertices(sparkBulkLoaderStateMachine);
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return nextState;
    }
}
