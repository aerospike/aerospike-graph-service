package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RECOVERY_FAILURE;

public class SparkBulkLoaderStateVerifyEdges extends SparkBulkLoaderState {
    public SparkBulkLoaderStateVerifyEdges(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        RecoveryUtil.updateState(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.EDGE_VERIFY);

        // TESTING USAGE ONLY
        final String recoveryFailure = sparkBulkLoaderStateMachine.config.getOrDefault(RECOVERY_FAILURE);
        if ("EDGE_VERIFY".equals(recoveryFailure)) {
            System.out.println("FOOOOOOOOOOOOObar " + recoveryFailure);
            throw new RuntimeException("Testing recovery failure.");
        } else {
            System.out.println("FOOOOOOOOOOOOO " + recoveryFailure);
        }

        sparkBulkLoaderStateMachine.edgeOperations.verifySampleEdgeAfterWrite(
                sparkBulkLoaderStateMachine.edgeDataset.sample(
                        DatasetOperations.getSamplingPercent(sparkBulkLoaderStateMachine.config)));
        sparkBulkLoaderStateMachine.progressBar.setEdgeValidationComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateCleanUp(sparkBulkLoaderStateMachine);
    }
}
