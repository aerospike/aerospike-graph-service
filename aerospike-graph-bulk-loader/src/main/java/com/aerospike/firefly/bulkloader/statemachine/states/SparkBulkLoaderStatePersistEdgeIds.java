package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.spark.sql.Column;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;

public class SparkBulkLoaderStatePersistEdgeIds extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStatePersistEdgeIds.class);
    public SparkBulkLoaderStatePersistEdgeIds(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Persist edge ids.
        // Persist Edge ID data to disk
        if (sparkBulkLoaderStateMachine.readOnly) {
            // Persisting Edge IDs is disabled. Do Nothing.
            LOGGER.debug("{} mode detected. System will not write persistent Edge IDs to temp storage.", READ_ONLY);
        } else {
            sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToStorage(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.edgeRecoveryDirectory,
                    sparkBulkLoaderStateMachine.fileConfig);

            // Now the order of ids in the partition should be preserved.
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.spark.read().option("header", "true").csv(sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
            sparkBulkLoaderStateMachine.edgeDataset.persist(StorageLevel.DISK_ONLY());

            // Latch recovery directory.
            RecoveryUtil.writeTempDirectory(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
        }

        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().getPartitions().length;
        LOGGER.info("EdgeId dataset have {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);

        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Update edge recovery info.
            RecoveryUtil.updateEdgeRecovery(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                    sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeDataset.repartition(
                    sparkBulkLoaderStateMachine.edgePartitionCount, new Column("~edgeid"));

            sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);
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
