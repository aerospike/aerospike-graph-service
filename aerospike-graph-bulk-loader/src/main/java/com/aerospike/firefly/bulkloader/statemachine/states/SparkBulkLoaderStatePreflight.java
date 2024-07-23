package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderPreflightException;

import java.util.Arrays;

public class SparkBulkLoaderStatePreflight extends SparkBulkLoaderState {
    public SparkBulkLoaderStatePreflight(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Preflight check
        DatasetOperations.preflightCheck(
                sparkBulkLoaderStateMachine.edgeDataset,
                sparkBulkLoaderStateMachine.vertexDataset,
                sparkBulkLoaderStateMachine.config);
        sparkBulkLoaderStateMachine.progressBar.setPreflightCheckComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStatePersistEdgeIds(sparkBulkLoaderStateMachine);
    }
}
