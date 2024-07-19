package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

import java.util.Arrays;

public class SparkBulkLoaderStatePreflight extends SparkBulkLoaderState {
    public SparkBulkLoaderStatePreflight(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    private Exception e = null;

    @Override
    public void executeState() {
        // Preflight check
        try {
            DatasetOperations.preflightCheck(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.vertexDataset,
                    sparkBulkLoaderStateMachine.config);
            sparkBulkLoaderStateMachine.PROGRESS_BAR.setPreflightCheckComplete();
        } catch (final Exception e) {
            // We are limiting stacktrace size by DRYRUN_STACKTRACE_LIMIT
            final StackTraceElement[] originalStackTrace = this.e.getStackTrace();
            final StackTraceElement[] limitedStackTrace =
                    Arrays.copyOf(originalStackTrace,
                            Math.min(originalStackTrace.length, sparkBulkLoaderStateMachine.DRYRUN_STACKTRACE_LIMIT));
            e.setStackTrace(limitedStackTrace);
            this.e = e;
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        if (e != null) {
            return new SparkBulkLoaderStateError(sparkBulkLoaderStateMachine, e);
        } else {
            return new SparkBulkLoaderStatePersistEdgeIds(sparkBulkLoaderStateMachine);
        }
    }
}
