package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

import java.nio.ByteBuffer;

abstract public class RecyclingEdgeIdManager<P extends BufferedNumericIdManager> implements IdManager<byte[]> {
    protected final long bufferSize;
    protected final P packingIdManager;
    protected final BufferedNumericIdManager uniqueIdManager;

    protected RecyclingEdgeIdManager(final P packingIdManager,
                                     final BufferedNumericIdManager uniqueIdManager,
                                     final long bufferSize) {
        this.packingIdManager = packingIdManager;
        this.uniqueIdManager = uniqueIdManager;
        this.bufferSize = bufferSize;
    }

    abstract public byte[] getNextId(final FireflyGraph graph);

    abstract protected byte[] getNewId(final FireflyGraph graph);

    abstract public void recycleId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted);

    static public byte[] longToBytes(final long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
