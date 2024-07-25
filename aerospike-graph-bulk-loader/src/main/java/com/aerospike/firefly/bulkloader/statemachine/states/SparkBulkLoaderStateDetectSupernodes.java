package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBulkLoaderStateDetectSupernodes extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateDetectSupernodes.class);
    public SparkBulkLoaderStateDetectSupernodes(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Supernode processing
        // Get the supernode threshold from Firefly config.
        final long onRecordIdLimit = sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph().ON_RECORD_ID_LIMIT;
        LOGGER.info("Supernode threshold: " + onRecordIdLimit);
        sparkBulkLoaderStateMachine.supernodes = sparkBulkLoaderStateMachine.edgeOperations.extractSupernodes(
                sparkBulkLoaderStateMachine.edgeDataset,
                onRecordIdLimit,
                sparkBulkLoaderStateMachine.incrementalLoad);
        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
    }
}
