package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateDone extends SparkBulkLoaderState {
    SparkBulkLoaderStateDone(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        throw new RuntimeException("Should not execute state Done");
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        throw new RuntimeException("Should not transition state Done");
    }
}
