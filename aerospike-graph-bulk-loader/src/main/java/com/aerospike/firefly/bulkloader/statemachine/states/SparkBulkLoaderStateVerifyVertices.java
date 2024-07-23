package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

public class SparkBulkLoaderStateVerifyVertices extends SparkBulkLoaderState {
    public SparkBulkLoaderStateVerifyVertices(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        RecoveryUtil.updateState(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.VERTEX_VERIFY);

        sparkBulkLoaderStateMachine.vertexOperations.verifySampleVerticesAfterWrite(
                sparkBulkLoaderStateMachine.vertexDataset.sample(
                        DatasetOperations.getSamplingPercent(sparkBulkLoaderStateMachine.config)));
        sparkBulkLoaderStateMachine.progressBar.setVertexValidationComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteEdges(sparkBulkLoaderStateMachine);
    }
}
