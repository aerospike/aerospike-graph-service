package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.BUCKET_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.STORAGE_ID_COLUMN;
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
        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().getPartitions().length;
        LOGGER.info("EdgeId dataset has {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);

        if (!sparkBulkLoaderStateMachine.readOnly) {
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToDataframe(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.edgeRecoveryDirectory,
                    sparkBulkLoaderStateMachine.fileConfig);

            // Latch recovery directory.
            RecoveryUtil.writeTempEdgeDirectory(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
        }

        if (sparkBulkLoaderStateMachine.isEdgeCacheWrittenWithVertex) {
            // Calculate after they are written so data is fresh.
            sparkBulkLoaderStateMachine.edgeCountAfterRemoval = sparkBulkLoaderStateMachine.edgeDataset.count();
            sparkBulkLoaderStateMachine.progressBar.setEdgeTotalCount(sparkBulkLoaderStateMachine.edgeCountAfterRemoval);

            final long allowedDetachedEdges = sparkBulkLoaderStateMachine.config.getOrDefaultInt(ALLOWED_BAD_EDGES_COUNT);
            LOGGER.info("Allowed detached edges: {}", allowedDetachedEdges);

            if ((sparkBulkLoaderStateMachine.edgeCount - sparkBulkLoaderStateMachine.edgeCountAfterRemoval) > allowedDetachedEdges) {
                throw new RuntimeException(BAD_EDGE_COUNT_EXCEEDED);
            }
        }

        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Update edge recovery info.
            RecoveryUtil.updateEdgeRecovery(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                    sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeDataset.repartitionByRange(
                    sparkBulkLoaderStateMachine.edgePartitionCount, new Column(BUCKET_ID_COLUMN));
            sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset.sortWithinPartitions(new Column(STORAGE_ID_COLUMN));
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
