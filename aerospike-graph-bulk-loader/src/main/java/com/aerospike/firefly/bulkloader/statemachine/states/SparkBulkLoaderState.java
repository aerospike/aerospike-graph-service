package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;

import java.util.Map;

public abstract class SparkBulkLoaderState {
    protected SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine;

    public SparkBulkLoaderState(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        this.sparkBulkLoaderStateMachine = sparkBulkLoaderStateMachine;
    }
    public abstract void executeState();
    public abstract SparkBulkLoaderState transitionState();
    protected abstract BulkLoadStateStatusMap getStateMap();
    public final Map<String, Object> getStateStatus() {
        return this.getStateMap();
    }
}
