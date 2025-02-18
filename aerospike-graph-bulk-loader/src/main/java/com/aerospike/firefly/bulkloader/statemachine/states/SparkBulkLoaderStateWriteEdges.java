package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

public class SparkBulkLoaderStateWriteEdges extends SparkBulkLoaderState {
    public SparkBulkLoaderStateWriteEdges(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        if (!sparkBulkLoaderStateMachine.readOnly) {
            RecoveryUtil.updateState(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.EDGE_WRITE);
        }

        // Write edges to Aerospike.
        // Clear incomplete partition data from summary updater in case we're resuming from a previously failed load.
        sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearEdgePartitionData();
        sparkBulkLoaderStateMachine.edgeOperations.writeEdgeToDB(
                sparkBulkLoaderStateMachine.edgeDataset,
                sparkBulkLoaderStateMachine.completedEdgePartitions,
                sparkBulkLoaderStateMachine.readOnly);
        sparkBulkLoaderStateMachine.progressBar.setEdgeLoadComplete();
        sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearEdgePartitionData();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateVerifyEdges(sparkBulkLoaderStateMachine);
    }
}
