package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStateReadVertices extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateReadVertices.class);

    SparkBulkLoaderStateReadVertices(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Load vertex dataset.
        sparkBulkLoaderStateMachine.vertexOperations = new VertexOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.vertexDirectories);
        sparkBulkLoaderStateMachine.vertexDataset = DatasetOperations.loadDataset(
                sparkBulkLoaderStateMachine.spark,
                sparkBulkLoaderStateMachine.vertexDirectories,
                VertexOperations.REQUIRED_VERTEX_HEADERS,
                DatasetOperations.getDfStorageLevel(sparkBulkLoaderStateMachine.config));
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateReadEdges(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("reading vertex data", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
