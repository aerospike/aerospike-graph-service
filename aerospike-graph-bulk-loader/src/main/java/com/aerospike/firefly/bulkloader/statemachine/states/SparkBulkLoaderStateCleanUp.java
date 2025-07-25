package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

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
        if (sparkBulkLoaderStateMachine.incrementalLoad) {
            sparkBulkLoaderStateMachine.progressBar.latchMergeInfo();
            sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearMergeVertexPartitionData();
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("cleanup", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
