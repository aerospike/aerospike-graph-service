package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.concurrent.atomic.AtomicLong;

public class DecrementingNumericIdManager extends BufferedNumericIdManager {
    protected DecrementingNumericIdManager(final String counterName, final long bufferSize) {
        super(counterName, bufferSize);
    }

    @Override
    protected Long getNextIdFromBuffers(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                                        final AtomicLong idTrigger) {
        final long id = idTracker.getAndDecrement();
        if (id <= idTrigger.get()) {
            // The ID we got is the last reserved ID so we need to buffer more
            bufferIds(graph, bufferSize, idTracker, idTrigger);
        }
        return id;
    }

    @Override
    protected void bufferIds(final FireflyGraph graph, long bufferSize, final AtomicLong idTracker,
                             final AtomicLong idTrigger) {
        LOG.info("Allocating batch of {} {}.", bufferSize, this.readableIdName);
        // This is the new last reserved ID
        idTrigger.set(graph.getBaseGraph().addIdCounter(this.counterName, -bufferSize));
        idTracker.set(idTrigger.get() + bufferSize - 1);
    }
}
