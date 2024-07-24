package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

public class SparkBulkLoaderStateVerifyEdges extends SparkBulkLoaderState {
    public SparkBulkLoaderStateVerifyEdges(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        RecoveryUtil.updateState(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.EDGE_VERIFY);

        // TESTING USAGE ONLY
        final String failureOnEdgeVerification = System.getProperty("bulkloader.testing.partition.failure.edge.verification");
        if (failureOnEdgeVerification != null && failureOnEdgeVerification.equals("true")) {
            throw new RuntimeException("Testing edge verification failure.");
        }

        sparkBulkLoaderStateMachine.edgeOperations.verifySampleEdgeAfterWrite(
                sparkBulkLoaderStateMachine.persistedEdgeIdDataset.sample(
                        DatasetOperations.getSamplingPercent(sparkBulkLoaderStateMachine.config)));
        sparkBulkLoaderStateMachine.edgeDataset.unpersist();
        sparkBulkLoaderStateMachine.progressBar.setEdgeValidationComplete();

        // TODO: Should clean up all recovery artifacts.
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }
}
