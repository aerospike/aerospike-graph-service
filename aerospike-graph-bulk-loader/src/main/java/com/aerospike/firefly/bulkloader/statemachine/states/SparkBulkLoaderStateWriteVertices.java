package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.ProgressBar;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.amazonaws.event.request.Progress;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStateWriteVertices extends SparkBulkLoaderState {
    private final ProgressBar progressBar;

    public SparkBulkLoaderStateWriteVertices(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
        this.progressBar = sparkBulkLoaderStateMachine.progressBar;
    }

    @Override
    public void executeState() {
        if (!sparkBulkLoaderStateMachine.readOnly) {
            RecoveryUtil.updateState(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.VERTEX_WRITE);
        }

        // Write vertices to Aerospike.
        // Clear incomplete partition data from summary updater in case we're resuming from a previously failed load.
        sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearVertexPartitionData();
        sparkBulkLoaderStateMachine.vertexOperations.writeVerticesToDB(
                sparkBulkLoaderStateMachine.vertexDataset,
                sparkBulkLoaderStateMachine.supernodes,
                sparkBulkLoaderStateMachine.completedVertexPartitions,
                sparkBulkLoaderStateMachine.readOnly);
        sparkBulkLoaderStateMachine.progressBar.setVertexLoadComplete();
        sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearVertexPartitionData();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateVerifyVertices(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("writing vertices", false, BULK_LOAD_STATUS_IN_PROGRESS,
                this.progressBar.getVertexPartitionWritePercentage(),
                this.progressBar.getVerticesWritten());
    }
}
