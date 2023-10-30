package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class RecyclingBufferedNumericIdManager implements IdManager<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RecyclingBufferedNumericIdManager.class);
    private final long bufferSize;
    private final BufferedNumericIdManager uniqueIdManager;
    private final BufferedNumericIdManager recyclingIdManager;
    private final List<Long> recycledIds = new LinkedList<>();
    private final boolean allowUserSupplied;

    public RecyclingBufferedNumericIdManager(final String recyclingIdCounterName,
                                             final String uniqueIdCounterName,
                                             final long bufferSize,
                                             final boolean allowUserSuppliedIds) {
        this.uniqueIdManager = new BufferedNumericIdManager(uniqueIdCounterName, bufferSize, allowUserSuppliedIds);
        this.recyclingIdManager = new BufferedNumericIdManager(recyclingIdCounterName, bufferSize, allowUserSuppliedIds);
        this.bufferSize = bufferSize;
        this.allowUserSupplied = allowUserSuppliedIds;
        if (bufferSize < 1) {
            throw new IllegalArgumentException("BufferedNumericIdManager bufferSize of '" + bufferSize + "' " +
                    "is not valid. The value must be greater than 0.");
        }
    }

    private byte[] longToBytes(final long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long bytesToLong(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    @Override
    public synchronized byte[] getNextId(final FireflyGraph graph) {
        final Long recycledId;
        synchronized (RecyclingBufferedNumericIdManager.class) {
            recycledId = recycledIds.isEmpty() ? recyclingIdManager.getNextId(graph) : recycledIds.remove(0);
        }
        final Long uniqueId = uniqueIdManager.getNextId(graph);
        final byte[] id = new byte[16];
        System.arraycopy(longToBytes(recycledId), 0, id, 0, 8);
        System.arraycopy(longToBytes(uniqueId), 0, id, 8, 8);
        return id;
    }

    @Override
    public boolean allow(final Class<?> id) {
        return allowUserSupplied && AerospikeConnection.IdToDiskTypeMap.containsKey(id);
    }

    @Override
    public void recycleId(final FireflyId id) {
        synchronized (RecyclingBufferedNumericIdManager.class) {
            // We have enough recycled IDs to buffer them
            final long recycledId;
            if (id instanceof FireflyPhatEdgeId) {
                recycledId = ((FireflyPhatEdgeId) id).getPackingId();
            } else if (id instanceof FireflyIdComposite) {
                recycledId = ((FireflyPhatEdgeId)((FireflyIdComposite) id).getEdgeId()).getPackingId();
            } else {
                final String message = "Could not recycle ID of unexpected type " + id.getClass().getName();
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }
            if (recycledIds.size() >= bufferSize) {
                LOG.warn("Recycled IDs buffer is full. Recycling ID " + recycledId + " will be dropped.");
                return;
            }
            recycledIds.add(recycledId);
        }
    }
}
