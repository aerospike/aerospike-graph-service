package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

public class SparkBulkLoaderStateWriteVertices extends SparkBulkLoaderState {
    public SparkBulkLoaderStateWriteVertices(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        if (!sparkBulkLoaderStateMachine.readOnly) {
            RecoveryUtil.updateState(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.VERTEX_WRITE);
        }

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
