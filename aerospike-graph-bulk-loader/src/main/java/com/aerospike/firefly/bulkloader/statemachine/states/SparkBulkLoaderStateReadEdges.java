package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStateReadEdges extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateReadEdges.class);

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
        Instant start = Instant.now();
        sparkBulkLoaderStateMachine.edgeCount = sparkBulkLoaderStateMachine.edgeDataset.count();
        LOGGER.info("Edge count: {} took {} ms",
                sparkBulkLoaderStateMachine.edgeCount,
                Duration.between(start, Instant.now()).toMillis());
        sparkBulkLoaderStateMachine.progressBar.setEdgeTotalCount(sparkBulkLoaderStateMachine.edgeCount);
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStatePreflight(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("processing edges", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
