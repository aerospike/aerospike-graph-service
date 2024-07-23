package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

import java.util.List;
import java.util.Set;

public class SparkBulkLoaderStateRecovery extends SparkBulkLoaderState {
    public SparkBulkLoaderStateRecovery(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Recover from checkpoint.
        RecoveryUtil.RecoveryInfo info = RecoveryUtil.recover(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph());

        // If supernodes are calculated, set them.
        if (info.getSupernodes() != null && !info.getSupernodes().isEmpty()) {
            sparkBulkLoaderStateMachine.initializerGraph.setSupernodes(info.supernodes);
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
    }
}
