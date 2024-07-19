package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateVerifyEdges extends SparkBulkLoaderState {
    public SparkBulkLoaderStateVerifyEdges(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        sparkBulkLoaderStateMachine.edgeOperations.verifySampleEdgeAfterWrite(
                sparkBulkLoaderStateMachine.persistedEdgeIdDataset.sample(
                        DatasetOperations.getSamplingPercent(sparkBulkLoaderStateMachine.config)));
        sparkBulkLoaderStateMachine.edgeDataset.unpersist();
        sparkBulkLoaderStateMachine.PROGRESS_BAR.setEdgeValidationComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }
}
