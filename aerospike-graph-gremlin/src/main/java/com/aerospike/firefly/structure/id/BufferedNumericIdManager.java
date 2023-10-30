package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class BufferedNumericIdManager implements IdManager<Long> {
    private final long bufferSize;
    private AtomicLong bufferedId = null;
    private long bufferTrigger;
    private final String counterName;
    private final boolean allowUserSupplied;

    public BufferedNumericIdManager(final String counterName, final long bufferSize, final boolean allowUserSupplied) {
        this.counterName = counterName;
        this.bufferSize = bufferSize;
        this.allowUserSupplied = allowUserSupplied;
        if (bufferSize < 1) {
            throw new IllegalArgumentException("BufferedNumericIdManager bufferSize of '" + bufferSize + "' " +
                    "is not valid. The value must be greater than 0.");
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

    private void bufferIds(final FireflyGraph graph) {
        // This is the new last reserved ID
        this.bufferTrigger = graph.getBaseGraph().decrementIdCounter(this.counterName, this.bufferSize);
        // Since Firefly returns decrementing negative long values as generated IDs, the first buffered ID to return is
        // the largest one
        this.bufferedId.set(this.bufferTrigger + bufferSize - 1);
    }

    @Override
    public boolean allow(final Class<?> id) {
        return allowUserSupplied && AerospikeConnection.IdToDiskTypeMap.containsKey(id);
    }

    @Override
    public void recycleId(final FireflyId id) {
        throw new RuntimeException("Recycling IDs is not supported by BufferedNumericIdManager.");
    }
}
