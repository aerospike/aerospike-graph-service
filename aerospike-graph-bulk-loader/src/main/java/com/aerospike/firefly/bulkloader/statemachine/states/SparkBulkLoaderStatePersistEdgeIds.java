package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.PACKING_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_EDGE_COUNT_EXCEEDED;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_EDGES_COUNT;

public class SparkBulkLoaderStatePersistEdgeIds extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStatePersistEdgeIds.class);

    public SparkBulkLoaderStatePersistEdgeIds(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        if (!sparkBulkLoaderStateMachine.readOnly) {
            LOGGER.info("Writing ids");
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToDataframe(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.edgeRecoveryDirectory,
                    sparkBulkLoaderStateMachine.fileConfig,
                    sparkBulkLoaderStateMachine.readOnly);

            // Latch recovery directory.
            LOGGER.info("Ids written");
            RecoveryUtil.writeTempEdgeDirectory(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
        }

        if (sparkBulkLoaderStateMachine.generateEdgeCaches) {
            // Calculate after they are written so data is fresh.
            // TODO: Remove.
            LOGGER.info("Starting edge counting");
            Instant start = Instant.now();
            sparkBulkLoaderStateMachine.edgeCountAfterRemoval = sparkBulkLoaderStateMachine.edgeDataset.count();
            LOGGER.info("Edge count after removal of dangling edges: {} took {} ms",
                    sparkBulkLoaderStateMachine.edgeCountAfterRemoval,
                    Duration.between(start, Instant.now()).toMillis());
            sparkBulkLoaderStateMachine.progressBar.setEdgeTotalCount(sparkBulkLoaderStateMachine.edgeCountAfterRemoval);

            final long allowedDetachedEdges = sparkBulkLoaderStateMachine.config.getOrDefaultInt(ALLOWED_BAD_EDGES_COUNT);
            LOGGER.info("Allowed detached edges: {}", allowedDetachedEdges);

            if ((sparkBulkLoaderStateMachine.edgeCount - sparkBulkLoaderStateMachine.edgeCountAfterRemoval) > allowedDetachedEdges) {
                throw new RuntimeException(BAD_EDGE_COUNT_EXCEEDED);
            }
        }

        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().getPartitions().length;
        LOGGER.info("EdgeId dataset has {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);

        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Update edge recovery info.
            RecoveryUtil.updateEdgeRecovery(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                    sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeDataset.repartition(
                    sparkBulkLoaderStateMachine.edgePartitionCount, new Column(PACKING_ID_COLUMN));
            sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset.sortWithinPartitions(new Column(PACKING_ID_COLUMN));
        }

        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("generating edge IDs", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
