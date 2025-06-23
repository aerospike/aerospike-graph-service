package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStateDetectSupernodes extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateDetectSupernodes.class);

    public SparkBulkLoaderStateDetectSupernodes(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        if (!sparkBulkLoaderStateMachine.readOnly) {
            RecoveryUtil.updateState(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.DETECT_SUPERNODES);
        }

        // This is a testing config, used to force failure in specific spots to allow us to test the recovery modes.
        final String failureOnSupernodes = sparkBulkLoaderStateMachine.config.getOrDefault(BulkLoaderConfigHelper.RECOVERY_FAILURE);
        if (RecoveryUtil.RecoveryState.DETECT_SUPERNODES.name().equals(failureOnSupernodes)) {
            throw new RuntimeException("Testing recovery failure, please contact support.");
        }

        // Supernode processing
        // Get the supernode threshold from Firefly config.
        final long onRecordIdLimit = sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph().ON_RECORD_ID_LIMIT;
        LOGGER.info("Supernode threshold: " + onRecordIdLimit);
        sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearSupernodePartitionData();
        sparkBulkLoaderStateMachine.supernodes = sparkBulkLoaderStateMachine.edgeOperations.extractSupernodes(
                sparkBulkLoaderStateMachine.edgeDataset,
                onRecordIdLimit,
                sparkBulkLoaderStateMachine.incrementalLoad,
                sparkBulkLoaderStateMachine.supernodeSamplingPercentage);
        if (!sparkBulkLoaderStateMachine.readOnly) {
            LOGGER.info("Writing supernode list to Aerospike for recovery.");
            RecoveryUtil.writeSupernodeList(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.supernodes);
        }
        sparkBulkLoaderStateMachine.progressBar.setSuperNodeExtractionComplete();
        sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearSupernodePartitionData();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateGenerateEdgeCaches(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("processing supernodes", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
