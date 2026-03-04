package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.structure.id.FireflyPhatEdgeId.getPhatEdgeStorageId;

public class DecrementingNumericIdManager extends BufferedNumericIdManager {
    private static final Logger LOG = LoggerFactory.getLogger(DecrementingNumericIdManager.class);
    private long lastReturnedId;
    private long newIdAtStart;
    private AtomicBoolean justBuffered = new AtomicBoolean(false);

    protected DecrementingNumericIdManager(final String counterName, final long bufferSize) {
        super(counterName, bufferSize);
    }

    @Override
    protected Long getNextIdFromBuffers(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                                        final AtomicLong idTrigger) {
        final long id = idTracker.getAndDecrement();
        if (justBuffered.getAndSet(false)) {
            newIdAtStart = id;
            final long packingId = getPhatEdgeStorageId(newIdAtStart, graph.getBaseGraph().getConfig().phatEdgeSize);
            LOG.error("ID {} that packs to {} is the new first returned ID after a new ID buffer", newIdAtStart, packingId);
        }
        if (id <= idTrigger.get()) {
            lastReturnedId = id;
            // The ID we got is the last reserved ID so we need to buffer more
            final long packingId = getPhatEdgeStorageId(lastReturnedId, graph.getBaseGraph().getConfig().phatEdgeSize);
            LOG.error("ID {} that packs to {} is the last returned ID before a new ID buffer", lastReturnedId, packingId);
            bufferIds(graph, bufferSize, idTracker, idTrigger);
        }
        return id;
    }

    @Override
    protected void bufferIds(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                             final AtomicLong idTrigger) {
        LOG.info("Allocating batch of {} {}.", bufferSize, this.readableIdName);
        // This is the new last reserved ID
        idTrigger.set(graph.getBaseGraph().addIdCounter(this.counterName, -bufferSize));
        idTracker.set(idTrigger.get() + bufferSize - 1);
        justBuffered.set(true);
    }
}
