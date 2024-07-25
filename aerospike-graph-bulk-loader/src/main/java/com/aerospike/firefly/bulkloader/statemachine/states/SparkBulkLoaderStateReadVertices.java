package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;
        LOGGER.info("Vertex dataset has {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);
        sparkBulkLoaderStateMachine.progressBar.setVertexPartitionCount(sparkBulkLoaderStateMachine.vertexPartitionCount);
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateReadEdges(sparkBulkLoaderStateMachine);
    }
}
