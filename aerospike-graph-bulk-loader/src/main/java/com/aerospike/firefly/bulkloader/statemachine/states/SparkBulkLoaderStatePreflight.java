package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

import java.util.Arrays;

public class SparkBulkLoaderStatePreflight extends SparkBulkLoaderState {
    private static final int DRYRUN_STACKTRACE_LIMIT = 5;

    public SparkBulkLoaderStatePreflight(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Preflight check
        try {
            DatasetOperations.preflightCheck(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.vertexDataset,
                    sparkBulkLoaderStateMachine.config);
        } catch (final Exception e) {
            // We are limiting stacktrace size by DRYRUN_STACKTRACE_LIMIT
            StackTraceElement[] originalStackTrace = e.getStackTrace();
            StackTraceElement[] limitedStackTrace =
                    Arrays.copyOf(originalStackTrace, Math.min(originalStackTrace.length, DRYRUN_STACKTRACE_LIMIT));
            e.setStackTrace(limitedStackTrace);
            throw e;
        }
        sparkBulkLoaderStateMachine.progressBar.setPreflightCheckComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStatePersistEdgeIds(sparkBulkLoaderStateMachine);
    }
}
