package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.PACKING_ID_COLUMN;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStatePersistEdgeIds extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStatePersistEdgeIds.class);
    public SparkBulkLoaderStatePersistEdgeIds(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Persist edge ids.
        // Persist Edge ID data to disk

        // Now the order of ids in the partition should be preserved.
        if (!sparkBulkLoaderStateMachine.readOnly) {
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToDataframe(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.edgeRecoveryDirectory,
                    sparkBulkLoaderStateMachine.fileConfig,
                    sparkBulkLoaderStateMachine.readOnly);

            // TODO: Does this make sense? we already persist in the above.
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.spark.read().option("header", "true").csv(sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
            sparkBulkLoaderStateMachine.edgeDataset.persist(StorageLevel.DISK_ONLY());
            sparkBulkLoaderStateMachine.edgeDataset.show(10, false);


            // Latch recovery directory.
            RecoveryUtil.writeTempEdgeDirectory(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
        }

        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().getPartitions().length;
        LOGGER.info("EdgeId dataset have {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);

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

    /// TODO: Incremental ??????

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("generating edge IDs", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
