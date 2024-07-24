package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;

public class SparkBulkLoaderStateReadVertices extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateReadVertices.class);

    SparkBulkLoaderStateReadVertices(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Load vertex dataset.
        sparkBulkLoaderStateMachine.vertexOperations = new VertexOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.vertexDirectories);
        sparkBulkLoaderStateMachine.vertexDataset = DatasetOperations.loadDataset(
                sparkBulkLoaderStateMachine.spark,
                sparkBulkLoaderStateMachine.vertexDirectories,
                VertexOperations.REQUIRED_VERTEX_HEADERS,
                DatasetOperations.getDfStorageLevel(sparkBulkLoaderStateMachine.config));
        sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;
        LOGGER.info("Vertex dataset has {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setVertexPartitionCount(sparkBulkLoaderStateMachine.vertexPartitionCount);
        RecoveryUtil.updateVertexRecovery(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                sparkBulkLoaderStateMachine.vertexPartitionCount);

        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Check that the temp directory to write to is set.
            String vertexRecoveryDirectory = null;
            try {
                vertexRecoveryDirectory = sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY) + "/recovery/vertex";
                //sparkBulkLoaderStateMachine.spark.sparkContext().setCheckpointDir(vertexRecoveryDirectory);
            } catch (final ConfigurationRuntimeException cre) {
                throw new RuntimeException(String.format("%s configuration key is empty. Please set %s in the configuration file or use the %s flag with caution.", TEMP_DIRECTORY_KEY, TEMP_DIRECTORY_KEY, READ_ONLY), cre);
            }

            //sparkBulkLoaderStateMachine.vertexDataset.checkpoint(true);
            sparkBulkLoaderStateMachine.vertexDataset.repartition(
                    sparkBulkLoaderStateMachine.vertexPartitionCount, new Column("~id")).
                    write().mode("overwrite").parquet(vertexRecoveryDirectory);
            //RecoveryUtil.updateVertexRecovery(
            //        sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
            //        sparkBulkLoaderStateMachine.spark.sparkContext().getCheckpointDir().get());
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateReadEdges(sparkBulkLoaderStateMachine);
    }
}
