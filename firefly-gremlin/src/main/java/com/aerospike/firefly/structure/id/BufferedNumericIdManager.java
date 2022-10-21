package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.concurrent.atomic.AtomicLong;

public class BufferedNumericIdManager<T extends FireflyElement> extends NumericIdManager<T> {
    private final long bufferSize;
    private AtomicLong bufferedId = null;
    private long bufferTrigger;
    public BufferedNumericIdManager(final Class<? extends FireflyElement> type, final String counterName,
                                    final long bufferSize) {
        super(type, counterName);
        if (bufferSize < 1) {
            this.bufferSize = 1;
        } else {
            this.bufferSize = bufferSize;
        }
    }

    @Override
    public synchronized Long getNextId(final FireflyGraph graph) {
        if (this.bufferedId == null) {
            this.bufferedId = new AtomicLong();
            bufferIds(graph);
            return getNextId(graph);
        }

        final long id = bufferedId.getAndDecrement();
        if (id <= this.bufferTrigger) {
            // The ID we got is the last reserved ID so we need to buffer more
            bufferIds(graph);
        }
        return id;
    }

    private void bufferIds(FireflyGraph graph) {
        // This is the new last reserved ID
        this.bufferTrigger = graph.getBaseGraph().decrementIdCounter(this.counterName ,this.bufferSize);
        // Since Firefly returns decrementing negative long values as generated IDs, the first buffered ID to return is
        // the largest one
        this.bufferedId.set(this.bufferTrigger + bufferSize - 1);
    }
}
