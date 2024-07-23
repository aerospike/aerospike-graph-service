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

        sparkBulkLoaderStateMachine.edgeOperations.verifySampleEdgeAfterWrite(
                sparkBulkLoaderStateMachine.persistedEdgeIdDataset.sample(
                        DatasetOperations.getSamplingPercent(sparkBulkLoaderStateMachine.config)));
        sparkBulkLoaderStateMachine.edgeDataset.unpersist();
        sparkBulkLoaderStateMachine.progressBar.setEdgeValidationComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }
}
