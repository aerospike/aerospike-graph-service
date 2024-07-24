package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;

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
        final String vertexCheckpoint;
        final String edgeCheckpoint;
        try {
            vertexCheckpoint = sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY) + "/recovery/vertex";
            edgeCheckpoint = sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY) + "/recovery/edge";
        } catch (final ConfigurationRuntimeException cre) {
            throw new RuntimeException(String.format("%s configuration key is empty. Please set %s in the configuration file or use the %s flag with caution.", TEMP_DIRECTORY_KEY, TEMP_DIRECTORY_KEY, READ_ONLY), cre);
        }
        //sparkBulkLoaderStateMachine.spark.sparkContext().setCheckpointDir(vertexCheckpoint);
        sparkBulkLoaderStateMachine.vertexDataset =
                sparkBulkLoaderStateMachine.spark.read().parquet(vertexCheckpoint);
        sparkBulkLoaderStateMachine.vertexDataset.repartition(info.getVertexPartitionCount(), new Column("~id"));
        //sparkBulkLoaderStateMachine.spark.sparkContext().setCheckpointDir(edgeCheckpoint);
        sparkBulkLoaderStateMachine.persistedEdgeIdDataset =
                sparkBulkLoaderStateMachine.spark.read().parquet(edgeCheckpoint);
        sparkBulkLoaderStateMachine.persistedEdgeIdDataset.repartition(info.getEdgePartitionCount(), new Column("~id"));

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
                    case "DETECT_SUPERNODES":
                        LOGGER.info("Recovering from detectSupernodes state");
                        // Here we have completed the preflight and persistentance of edge ids.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoints.
                        nextState = new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
                        break;
                    case "VERTEX_WRITE":
                        LOGGER.info("Recovering from writeVertices state");
                        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
                        // Here we have completed the supernode detection and died during vertex writing.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoint.
                        // We can also load the supernodes and vertex partitions.
                        nextState = new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
                        break;
                    case "VERTEX_VERIFY":
                        LOGGER.info("Recovering from verifyVertices state");
                        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
                        sparkBulkLoaderStateMachine.progressBar.setVertexLoadComplete();
                        // Here we have completed the vertex writing and died during vertex verification.
                        // Therefore, we can reload the edge dataset and vertex dataset checkpoint.
                        // We can also load the supernodes.
                        // Vertex partitions are irrelevant, and we can restart the vertex verification step.
                        nextState = new SparkBulkLoaderStateVerifyVertices(sparkBulkLoaderStateMachine);
                        break;
                    case "EDGE_WRITE":
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
                    case "EDGE_VERIFY":
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
                        break;
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
