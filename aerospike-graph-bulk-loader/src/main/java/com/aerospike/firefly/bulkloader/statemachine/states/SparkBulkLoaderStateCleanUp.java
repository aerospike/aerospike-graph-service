package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

public class SparkBulkLoaderStateCleanUp extends SparkBulkLoaderState {
    public SparkBulkLoaderStateCleanUp(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Clean up all recovery artifacts.
        sparkBulkLoaderStateMachine.edgeDataset.unpersist();
        sparkBulkLoaderStateMachine.vertexDataset.unpersist();
        sparkBulkLoaderStateMachine.persistedEdgeIdDataset.unpersist();
        sparkBulkLoaderStateMachine.vertexDataset.unpersist();
        RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }
}
