package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateStart extends SparkBulkLoaderState {
    public SparkBulkLoaderStateStart(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }
    @Override
    public void executeState() {
        // Stub for now, will need to use later for resuming.
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateReadVertices(sparkBulkLoaderStateMachine);
    }
}
