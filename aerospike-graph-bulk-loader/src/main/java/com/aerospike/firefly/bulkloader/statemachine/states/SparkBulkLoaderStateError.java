package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateError extends SparkBulkLoaderState {
    public final String errorMessage;
    public final Exception e;

    public SparkBulkLoaderStateError(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine,
                                     final String errorMessage) {
        super(sparkBulkLoaderStateMachine);
        this.errorMessage = errorMessage;
        this.e = null;
    }

    public SparkBulkLoaderStateError(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine,
                                     final Exception e) {
        super(sparkBulkLoaderStateMachine);
        this.errorMessage = e.getMessage();
        this.e = e;
    }
    @Override
    public void executeState() {
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return this;
    }
}
