package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

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
        final boolean edgeIdWriteDisabled = sparkBulkLoaderStateMachine.config.hasAction(READ_ONLY);
        String writeLocation = null;
        if (edgeIdWriteDisabled) {
            // Persisting Edge IDs is disabled. Do Nothing.
            LOGGER.debug("{} mode detected. System will not write persistent Edge IDs to temp storage.", READ_ONLY);
        } else {
            // Check that the temp directory to write to is set.
            try {
                writeLocation = sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY);
            } catch (final ConfigurationRuntimeException cre) {
                throw new RuntimeException(String.format("%s is empty. Please set %s in the configuration file or use the %s flag with caution.", TEMP_DIRECTORY_KEY, TEMP_DIRECTORY_KEY, READ_ONLY), cre);
            }
            final String dirSeperator = sparkBulkLoaderStateMachine.fileSystem.equals(sparkBulkLoaderStateMachine.local)
                    ? File.separator : "/";
            final String tempEdgeDir = RandomStringUtils.randomAlphanumeric(8);
            writeLocation =  writeLocation.endsWith(dirSeperator) ? writeLocation + tempEdgeDir : writeLocation + dirSeperator + tempEdgeDir;
            sparkBulkLoaderStateMachine.configureFileSystem(
                    sparkBulkLoaderStateMachine.spark,
                    sparkBulkLoaderStateMachine.cmd, writeLocation);
            sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToStorage(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    writeLocation,
                    sparkBulkLoaderStateMachine.fileConfig);

        }
        // If Edge ID persistence mode was disabled, read directly from Edge CSVs - else read written Edge IDs from disk.
        final Dataset<Row> edgeIdDataset = edgeIdWriteDisabled ?
                sparkBulkLoaderStateMachine.edgeDataset :
                sparkBulkLoaderStateMachine.spark.read().option("header", "true").csv(writeLocation);

        sparkBulkLoaderStateMachine.edgePartitionCount = edgeIdDataset.rdd().getPartitions().length;
        LOGGER.info("EdgeId dataset have {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.persistedEdgeIdDataset = DatasetOperations.persistIfPossible(
                DatasetOperations.getDfStorageLevel(sparkBulkLoaderStateMachine.config), edgeIdDataset);
        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
    }
}
