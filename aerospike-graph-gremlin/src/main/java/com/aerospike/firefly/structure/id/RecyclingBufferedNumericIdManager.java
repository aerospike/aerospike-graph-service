package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class RecyclingBufferedNumericIdManager implements IdManager<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RecyclingBufferedNumericIdManager.class);
    protected final long bufferSize;
    protected final BufferedNumericIdManager newIdManager;
    protected final BufferedNumericIdManager recyclingIdManager;
    protected final ConcurrentLinkedQueue<Long> recycledIds = new ConcurrentLinkedQueue<>();

    protected RecyclingBufferedNumericIdManager(final String recyclingIdCounterName,
                                                final String newIdCounterName,
                                                final long bufferSize,
                                                final long recycleBufferSize) {
        this.newIdManager = new BufferedNumericIdManager(newIdCounterName, bufferSize);
        this.recyclingIdManager = new BufferedNumericIdManager(recyclingIdCounterName, recycleBufferSize);
        this.bufferSize = bufferSize;
    }

    static public byte[] longToBytes(final long x) {
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
        if (this.recycledIds.peek() == null) {
            return getNewId(graph);
        } else {
            return getRecycledId(graph);
        }
    }

    protected byte[] getNewId(final FireflyGraph graph) {
        final byte[] id = new byte[8];
        final Long newId = this.newIdManager.getNextId(graph);
        System.arraycopy(longToBytes(newId), 0, id, 0, 8);
        return id;
    }

    protected byte[] getRecycledId(final FireflyGraph graph) {
        final byte[] id = new byte[16];
        final Long recycledPackingId = this.recycledIds.poll();
        final Long recycledUniqueId = this.recyclingIdManager.getNextId(graph);
        System.arraycopy(longToBytes(recycledPackingId), 0, id, 0, 8);
        System.arraycopy(longToBytes(recycledUniqueId), 0, id, 8, 8);
        return id;
    }

    @Override
    public void recycleId(final FireflyId id) {
        final long recycledId;
        if (id instanceof FireflyPhatEdgeId) {
            recycledId = ((FireflyPhatEdgeId) id).getPackingId();
        } else {
            final String message = "Could not recycle ID of unexpected type " + id.getClass().getName();
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        if (this.recycledIds.size() >= bufferSize) {
            LOG.warn("Recycled IDs buffer is full. Recycling ID {} will be dropped.", recycledId);
            return;
        }
        this.recycledIds.add(recycledId);
    }

    public int availableRecycledIds() {
        return this.recycledIds.size();
    }
}
