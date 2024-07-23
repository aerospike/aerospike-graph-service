package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

public class SparkBulkLoaderStateWriteVertices extends SparkBulkLoaderState {
    public SparkBulkLoaderStateWriteVertices(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Write vertices to Aerospike.
        sparkBulkLoaderStateMachine.vertexOperations.writeVerticesToDB(
                sparkBulkLoaderStateMachine.vertexDataset,
                sparkBulkLoaderStateMachine.supernodes,
                sparkBulkLoaderStateMachine.completedVertexPartitions,
                sparkBulkLoaderStateMachine.readOnly);
        sparkBulkLoaderStateMachine.progressBar.setVertexLoadComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateVerifyVertices(sparkBulkLoaderStateMachine);
    }
}
