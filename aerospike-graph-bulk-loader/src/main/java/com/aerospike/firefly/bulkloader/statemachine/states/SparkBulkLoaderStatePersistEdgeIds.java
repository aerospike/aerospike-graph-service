package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.EDGE_ID_COLUMN;
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
            // Check that the temp directory to write to is set.
            String edgeRecoveryDirectory;
            try {
                edgeRecoveryDirectory = RecoveryUtil.getEdgeRecoveryDirectory(
                        sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY),
                        sparkBulkLoaderStateMachine.fileSystem.equals(SparkBulkLoaderStateMachine.LOCAL)
                                ? File.separator : "/");
            } catch (final ConfigurationRuntimeException cre) {
                throw new RuntimeException(String.format("%s configuration key is empty. " +
                        "Please set %s in the configuration file or use the %s flag with caution.",
                        TEMP_DIRECTORY_KEY, TEMP_DIRECTORY_KEY, READ_ONLY), cre);
            }

            // Configure file system and write edge ids to storage.
            sparkBulkLoaderStateMachine.configureFileSystem(
                    sparkBulkLoaderStateMachine.spark,
                    sparkBulkLoaderStateMachine.cmd, edgeRecoveryDirectory);
            sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToStorage(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    edgeRecoveryDirectory,
                    sparkBulkLoaderStateMachine.fileConfig);

            // Now the order of ids in the partition should be preserved.
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.spark.read().option("header", "true").csv(edgeRecoveryDirectory);
            sparkBulkLoaderStateMachine.edgeDataset.persist(StorageLevel.DISK_ONLY());
            sparkBulkLoaderStateMachine.edgeDataset.repartition(new Column("~edgeid"));
        }

        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().getPartitions().length;

        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Update edge recovery info.
            RecoveryUtil.updateEdgeRecovery(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                    sparkBulkLoaderStateMachine.edgePartitionCount);
        }

        LOGGER.info("EdgeId dataset have {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);

        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
    }
}
