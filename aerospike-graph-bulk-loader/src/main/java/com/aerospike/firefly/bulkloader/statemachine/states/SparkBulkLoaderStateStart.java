package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.CLEAR_EXISTING_DATA_EMPTY_DATABASE;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DATABASE_NOT_EMPTY;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.INCREMENTAL_AND_CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.INCREMENTAL_AND_RECOVERY_INFO_NO_RESUME_FLAG;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.INCREMENTAL_LOAD_EMPTY_DATABASE;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.RECOVERY_INFO_NO_CLEAR_EXISTING_DATA_FLAG_OR_RESUME;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.RESUME_AND_CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.RESUME_WITHOUT_RECOVERY_INFO;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_EDGE_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_VERTEX_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.FORCE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;

public class SparkBulkLoaderStateStart extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateStart.class);
    private SparkBulkLoaderState nextState = null;

    public SparkBulkLoaderStateStart(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    private void loadVertexDataset(final RecoveryUtil.RecoveryInfo info) {
        sparkBulkLoaderStateMachine.vertexPartitionCount = info.getVertexPartitionCount();

        // Load vertex dataset.
        sparkBulkLoaderStateMachine.vertexOperations = new VertexOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.vertexDirectories);
        sparkBulkLoaderStateMachine.vertexDataset = DatasetOperations.loadDataset(
                sparkBulkLoaderStateMachine.spark,
                sparkBulkLoaderStateMachine.vertexDirectories,
                VertexOperations.REQUIRED_VERTEX_HEADERS,
                DatasetOperations.getDfStorageLevel(sparkBulkLoaderStateMachine.config));

        // Set progress bar info.
        LOGGER.info("Vertex dataset has {} partitions", sparkBulkLoaderStateMachine.vertexPartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setVertexPartitionCount(sparkBulkLoaderStateMachine.vertexPartitionCount);

        // Repartition the vertex dataset.
        sparkBulkLoaderStateMachine.vertexDataset = sparkBulkLoaderStateMachine.vertexDataset.repartition(
                sparkBulkLoaderStateMachine.vertexPartitionCount, new Column("~id"));
    }

    private void loadEdgeDataset(final RecoveryUtil.RecoveryInfo info) {
        // Load edge operations.
        sparkBulkLoaderStateMachine.edgeOperations = new EdgeOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.edgeDirectories);

        final String edgeRecoveryDirectory = info.getTempDirectory();
        sparkBulkLoaderStateMachine.edgeDirectories = sparkBulkLoaderStateMachine.getDirectories(
                sparkBulkLoaderStateMachine.spark,
                sparkBulkLoaderStateMachine.cmd,
                edgeRecoveryDirectory);
        sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.spark.
                read().option("header", "true").csv(edgeRecoveryDirectory);

        sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeDataset.repartition(
                info.getEdgePartitionCount(), new Column("~edgeid"));

        sparkBulkLoaderStateMachine.edgePartitionCount = info.getEdgePartitionCount();
        LOGGER.info("EdgeId dataset has {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);

        RecoveryUtil.updateEdgeRecovery(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();
    }

    private void loadCheckpointDatasets(final RecoveryUtil.RecoveryInfo info) {
        // Load datasets.
        loadVertexDataset(info);
        loadEdgeDataset(info);

        // Load the supernodes and partitions.
        sparkBulkLoaderStateMachine.supernodes = info.getSupernodes();
        sparkBulkLoaderStateMachine.edgeOperations.setSupernodes(sparkBulkLoaderStateMachine.supernodes);
        sparkBulkLoaderStateMachine.completedVertexPartitions = info.getVertexPartitions();
        sparkBulkLoaderStateMachine.completedEdgePartitions = info.getEdgePartitions();

        // Update progress bar.
        sparkBulkLoaderStateMachine.progressBar.setPreflightCheckComplete();
        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();

        // Calculate the number of partitions.
        sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;
        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().partitions().length;
    }

    @Override
    public void executeState() {
        // Check to see if actions are valid.
        final boolean forceFlag = sparkBulkLoaderStateMachine.config.hasAction(FORCE);
        final boolean resumeFlag = sparkBulkLoaderStateMachine.config.hasAction(RESUME);
        final boolean incrementalLoadFlag = sparkBulkLoaderStateMachine.config.hasAction(INCREMENTAL_LOAD);
        final boolean clearExistingDataFlag = sparkBulkLoaderStateMachine.config.hasAction(CLEAR_EXISTING_DATA);
        if (forceFlag) {
            LOGGER.info("Force flag detected; removing recovery data before starting.");
            RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph);
            LOGGER.info("Recovery data removed; continuing load.");
        }

        RecoveryUtil.RecoveryInfo info =
                RecoveryUtil.recover(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());
        final boolean recoveryInfoExists = info.getState() != null;

        // Cannot set both incremental and clear existing data flags or resume and clear existing data flags.
        if (incrementalLoadFlag && clearExistingDataFlag) {
            throw new IllegalArgumentException(INCREMENTAL_AND_CLEAR_EXISTING_DATA);
        }
        if (resumeFlag && clearExistingDataFlag) {
            throw new IllegalArgumentException(RESUME_AND_CLEAR_EXISTING_DATA);
        }

        // Cannot set incremental load when database is empty.
        if (incrementalLoadFlag && sparkBulkLoaderStateMachine.initializerGraph.isEmpty()) {
            throw new IllegalStateException(INCREMENTAL_LOAD_EMPTY_DATABASE);
        }

        if (resumeFlag && !recoveryInfoExists) {
            throw new IllegalStateException(RESUME_WITHOUT_RECOVERY_INFO);
        }

        // Clear existing data is not valid if graph is empty and no recovery info exists.
        if (clearExistingDataFlag && (sparkBulkLoaderStateMachine.initializerGraph.isEmpty() && !recoveryInfoExists)) {
            throw new IllegalStateException(CLEAR_EXISTING_DATA_EMPTY_DATABASE);
        } else if (clearExistingDataFlag) {
            RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph);
            sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph().dropDatabase(sparkBulkLoaderStateMachine.initializerGraph, false);

            // Reload recovery info after truncating the database, should be nulled out now.
            info = RecoveryUtil.recover(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());
        } else {
            if (recoveryInfoExists && !resumeFlag && incrementalLoadFlag) {
                throw new IllegalStateException(INCREMENTAL_AND_RECOVERY_INFO_NO_RESUME_FLAG);
            } else if (recoveryInfoExists && !resumeFlag) {
                throw new IllegalStateException(RECOVERY_INFO_NO_CLEAR_EXISTING_DATA_FLAG_OR_RESUME);
            }
        }

        // Recover from checkpoint.
        if (!sparkBulkLoaderStateMachine.readOnly) {
            final String state = info.getState();
            if (state != null && !state.isEmpty()) {
                loadCheckpointDatasets(info);
                sparkBulkLoaderStateMachine.progressBar.setResumeableLoadComplete();
                switch (state) {
                    case "DETECT_SUPERNODES":
                        LOGGER.info("Recovering from detectSupernodes state");
                        // Here we have completed the preflight and persistence of edge ids.
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
                        throw new IllegalStateException("Error during bulk load recovery - unknown state: " + state);
                }
            } else {
                if (!sparkBulkLoaderStateMachine.initializerGraph.isEmpty() &&
                        !sparkBulkLoaderStateMachine.config.hasAction(DISABLE_EDGE_WRITE) &&
                        !sparkBulkLoaderStateMachine.config.hasAction(DISABLE_VERTEX_WRITE) &&
                        !sparkBulkLoaderStateMachine.config.hasAction(INCREMENTAL_LOAD)) {
                    // If we're doing partial writing checking the emptiness of the database isn't valid.
                    LOGGER.error(DATABASE_NOT_EMPTY);
                    throw new RuntimeException(DATABASE_NOT_EMPTY);
                }

                // Fresh load, truncate any metadata.
                RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph);

                // No state found, start from the beginning.
                LOGGER.info("Unable to find state to recover from. Starting from the beginning.");
                nextState = new SparkBulkLoaderStateReadVertices(sparkBulkLoaderStateMachine);
            }
        } else {
            if (!sparkBulkLoaderStateMachine.initializerGraph.isEmpty() &&
                    !sparkBulkLoaderStateMachine.config.hasAction(DISABLE_EDGE_WRITE) &&
                    !sparkBulkLoaderStateMachine.config.hasAction(DISABLE_VERTEX_WRITE) &&
                    !sparkBulkLoaderStateMachine.config.hasAction(INCREMENTAL_LOAD)) {
                // If we're doing partial writing checking the emptiness of the database isn't valid.
                LOGGER.error(DATABASE_NOT_EMPTY);
                throw new RuntimeException(DATABASE_NOT_EMPTY);
            }

            // Fresh load, truncate any metadata.
            RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph);

            // Read only mode, start from the beginning.
            LOGGER.info("Read only mode detected. Starting from the beginning.");
            nextState = new SparkBulkLoaderStateReadVertices(sparkBulkLoaderStateMachine);
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return nextState;
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("initializing", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
