package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;

public class SparkBulkLoaderStateCleanUp extends SparkBulkLoaderState {
    public SparkBulkLoaderStateCleanUp(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Clean up all recovery artifacts.
        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Delete the temp directory.
            try {
                // TODO: If cleanup set?
                String edgeRecoveryDirectory;
                try {
                    edgeRecoveryDirectory = RecoveryUtil.getEdgeRecoveryDirectory(
                            sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY),
                            sparkBulkLoaderStateMachine.fileSystem.equals(SparkBulkLoaderStateMachine.LOCAL)
                                    ? File.separator : "/");
                } catch (final ConfigurationRuntimeException cre) {
                    // TODO: better msg.
                    throw new RuntimeException(String.format("%s configuration key is empty. Please set %s in the configuration file or use the %s flag with caution.", TEMP_DIRECTORY_KEY, TEMP_DIRECTORY_KEY, READ_ONLY), cre);
                }
                final Configuration conf = sparkBulkLoaderStateMachine.spark.sparkContext().hadoopConfiguration();
                if (SparkBulkLoaderStateMachine.GCS.equalsIgnoreCase(sparkBulkLoaderStateMachine.fileSystem)) {
                    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
                }
                final FileSystem fs = FileSystem.get(conf);
                final Path tempDir = new Path(edgeRecoveryDirectory);
                if (fs.exists(tempDir)) {
                    fs.delete(tempDir, true);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get FileSystem", e);
            }
            sparkBulkLoaderStateMachine.edgeDataset.unpersist();
        }
        RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }
}
