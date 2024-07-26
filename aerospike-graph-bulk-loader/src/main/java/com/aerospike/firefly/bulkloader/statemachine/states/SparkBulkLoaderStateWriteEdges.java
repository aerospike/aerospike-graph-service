package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateWriteEdges extends SparkBulkLoaderState {
    public SparkBulkLoaderStateWriteEdges(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Write edges to Aerospike.
        sparkBulkLoaderStateMachine.edgeOperations.writeEdgeToDB(sparkBulkLoaderStateMachine.persistedEdgeIdDataset);
        sparkBulkLoaderStateMachine.progressBar.setEdgeLoadComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateVerifyEdges(sparkBulkLoaderStateMachine);
    }
}
