package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

public class SparkBulkLoaderStateCleanUp extends SparkBulkLoaderState {
    public SparkBulkLoaderStateCleanUp(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Clean up recovery artifacts in spark and Aerospike.
        // User must cleanup their own data in HDFS.
        if (!sparkBulkLoaderStateMachine.readOnly) {
            sparkBulkLoaderStateMachine.edgeDataset.unpersist();
            RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph);
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }
}
