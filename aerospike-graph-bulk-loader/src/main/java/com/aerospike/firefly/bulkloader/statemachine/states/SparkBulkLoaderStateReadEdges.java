package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;

import javax.xml.crypto.Data;

public class SparkBulkLoaderStateReadEdges extends SparkBulkLoaderState {

    SparkBulkLoaderStateReadEdges(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Load edge dataset.
        sparkBulkLoaderStateMachine.edgeOperations = new EdgeOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.edgeDirectories);
        sparkBulkLoaderStateMachine.edgeDataset = DatasetOperations.loadDataset(
                sparkBulkLoaderStateMachine.spark,
                sparkBulkLoaderStateMachine.edgeDirectories,
                EdgeOperations.REQUIRED_EDGE_HEADERS,
                DatasetOperations.getDfStorageLevel(sparkBulkLoaderStateMachine.config));
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStatePreflight(sparkBulkLoaderStateMachine);
    }
}
