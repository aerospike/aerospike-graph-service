package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.structure.id.FireflyPhatEdgeId.getPhatEdgeStorageId;

public class MrtEdgePackingIdManager extends DecrementingNumericIdManager {
    private boolean isIdBufferContinuous = true;
    private Long lastReturnedId = null;

    protected MrtEdgePackingIdManager(final String counterName, final long bufferSize) {
        super(counterName, bufferSize);
    }

    @Override
    protected Long getNextIdFromBuffers(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                                        final AtomicLong idTrigger) {
        // Capture the idTracker BEFORE calling super, so bufferIds can use it if a refill is triggered
        this.lastReturnedId = idTracker.get();
        return super.getNextIdFromBuffers(graph, bufferSize, idTracker, idTrigger);
    }

    @Override
    protected void bufferIds(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                             final AtomicLong idTrigger) {
        super.bufferIds(graph, bufferSize, idTracker, idTrigger);
        if (lastReturnedId != null) {
            final long packingSize = graph.getBaseGraph().getConfig().phatEdgeSize;
            final long postBufferId = idTracker.get();
            final Long postBufferRecordId = getPhatEdgeStorageId(postBufferId, packingSize);
            final Long preBufferRecordId = getPhatEdgeStorageId(this.lastReturnedId, packingSize);
            this.isIdBufferContinuous = postBufferRecordId.equals(preBufferRecordId);
        }
    }

    /**
     * When buffering new packing IDs it may be the case that the new IDs are not continuous from pre-buffering.
     * Use this to check if the next ID can be used for the same Edge Pack (has the same Record ID).
     * This also resets the flag.
     *
     * @return true if the next ID to be returned after buffering IDs packs to the same Record ID as the last ID
     */
    public boolean isIdBufferContinuous() {
        if (!this.isIdBufferContinuous) {
            this.isIdBufferContinuous = true;
            return false;
        } else {
            return true;
        }
    }
}
