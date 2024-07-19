package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateError extends SparkBulkLoaderState {
    public SparkBulkLoaderStateError(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine,
                                     final String errorMessage) {
        super(sparkBulkLoaderStateMachine);
    }

    public SparkBulkLoaderStateError(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine,
                                     final Exception e) {
        super(sparkBulkLoaderStateMachine);
    }
    @Override
    public void executeState() {
        System.out.println("ERR");
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return null;
    }
}
