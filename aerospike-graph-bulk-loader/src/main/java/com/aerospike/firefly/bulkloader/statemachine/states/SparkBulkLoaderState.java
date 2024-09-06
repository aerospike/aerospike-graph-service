package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public abstract class SparkBulkLoaderState {
    protected SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine;

    public SparkBulkLoaderState(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        this.sparkBulkLoaderStateMachine = sparkBulkLoaderStateMachine;
    }
    public abstract void executeState();
    public abstract SparkBulkLoaderState transitionState();
}
