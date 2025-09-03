package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

import java.nio.ByteBuffer;

abstract public class RecyclingEdgeIdManager implements IdManager<byte[]> {
    protected final long bufferSize;
    protected final BufferedNumericIdManager packingIdManager;
    protected final BufferedNumericIdManager uniqueIdManager;

    protected RecyclingEdgeIdManager(final String uniqueIdCounterName,
                                     final String packingIdCounterName,
                                     final long bufferSize,
                                     final long recycleBufferSize) {
        this.packingIdManager = new DecrementingNumericIdManager(packingIdCounterName, bufferSize);
        this.uniqueIdManager = new IncrementingNumericIdManager(uniqueIdCounterName, recycleBufferSize);
        this.bufferSize = bufferSize;
    }

    abstract public byte[] getNextId(final FireflyGraph graph);

    abstract protected byte[] getNewId(final FireflyGraph graph);

    abstract public void recycleId(final FireflyId id, final FireflyGraph graph);

    static public byte[] longToBytes(final long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
