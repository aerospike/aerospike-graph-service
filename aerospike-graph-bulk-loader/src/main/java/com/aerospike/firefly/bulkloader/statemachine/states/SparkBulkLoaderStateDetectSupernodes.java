package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
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
        if (!sparkBulkLoaderStateMachine.readOnly) {
            LOGGER.info("Writing supernode list to Aerospike for recovery.");
            RecoveryUtil.writeSupernodeList(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.supernodes);
        }
        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
    }
}
