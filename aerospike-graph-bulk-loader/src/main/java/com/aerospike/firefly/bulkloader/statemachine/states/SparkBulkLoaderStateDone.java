package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;

import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors.BAD_EDGE;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors.BAD_ENTRY;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors.DUPLICATE_VID;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_SUCCESS;

public class SparkBulkLoaderStateDone extends SparkBulkLoaderState {
    private final long duplicateVertexIdCount;
    private final long badEdgeCount;
    private final long badEntryCount;

    SparkBulkLoaderStateDone(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
        final AerospikeConnection db = sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph();
        this.badEntryCount = db.incrementAndGetBadEntryCount(0);
        this.duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
        this.badEdgeCount = db.incrementAndGetBadEdgeCount(0);
    }

    @Override
    public void executeState() {
        throw new RuntimeException("Should not execute state Done");
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        throw new RuntimeException("Should not transition state Done");
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        final BulkLoadStateStatusMap stateMap = new BulkLoadStateStatusMap("done", true, BULK_LOAD_STATUS_SUCCESS);
        stateMap.put(DUPLICATE_VID, duplicateVertexIdCount);
        stateMap.put(BAD_ENTRY, badEntryCount);
        stateMap.put(BAD_EDGE, badEdgeCount);
        return stateMap;
    }
}
