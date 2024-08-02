package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RECOVERY_FAILURE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;

public class SparkBulkLoaderStateVerifyVertices extends SparkBulkLoaderState {
    public SparkBulkLoaderStateVerifyVertices(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        RecoveryUtil.updateState(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.VERTEX_VERIFY);

        // TESTING USAGE ONLY
        final String recoveryFailure = sparkBulkLoaderStateMachine.config.getOrDefault(RECOVERY_FAILURE);
        if ("VERTEX_VERIFY".equals(recoveryFailure)) {
            throw new RuntimeException("Testing recovery failure.");
        }

        sparkBulkLoaderStateMachine.vertexOperations.verifySampleVerticesAfterWrite(
                sparkBulkLoaderStateMachine.vertexDataset.sample(
                        DatasetOperations.getSamplingPercent(sparkBulkLoaderStateMachine.config)));
        sparkBulkLoaderStateMachine.progressBar.setVertexValidationComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteEdges(sparkBulkLoaderStateMachine);
    }
}
