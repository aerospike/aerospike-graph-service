package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateStart extends SparkBulkLoaderState {
    public SparkBulkLoaderStateStart(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }
    @Override
    public void executeState() {
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateReadVertices(sparkBulkLoaderStateMachine);
    }
}
