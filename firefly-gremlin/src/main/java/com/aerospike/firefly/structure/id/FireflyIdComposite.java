package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyEdge;

import java.nio.ByteBuffer;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdComposite extends FireflyId {
    private final byte[] id;
    private final FireflyId edgeId;

    public FireflyIdComposite(final FireflyId edgeId, final FireflyId inVertexId, final FireflyId outVertexId) {
        this.id = new byte[Long.BYTES * 3];
        final byte[] eId = longToBytes((Long) edgeId.getStorageId());
        final byte[] inId = longToBytes((Long) inVertexId.getStorageId());
        final byte[] outId = longToBytes((Long) outVertexId.getStorageId());
        for (int i = 0; i < Long.BYTES; i++) {
            this.id[i] = eId[i];
            this.id[i + Long.BYTES] = inId[i];
            this.id[i + Long.BYTES * 2] = outId[i];
        }
        this.edgeId = edgeId;
    }

    public FireflyIdComposite(final byte[] ids) {
        id = ids;
        this.edgeId = FireflyIdFactory.createFromUser(FireflyEdge.class, longFromBytes(0));
    }

    private byte[] longToBytes(final long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long longFromBytes(final int idx) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(id, idx, Long.BYTES);
        buffer.flip();
        return buffer.getLong();
    }

    public FireflyId getEdgeId() {
        return new FireflyIdNumeric(longFromBytes(0));
    }

    public FireflyId getInVertexId() {
        return FireflyIdFactory.createId(longFromBytes(Long.BYTES));
    }

    public FireflyId getOutVertexId() {
        return FireflyIdFactory.createId(longFromBytes(Long.BYTES * 2));
    }


    @Override
    public Object getUserId() {
        return edgeId.getUserId();
    }

    @Override
    public Object getStorageId() {
        return longFromBytes(0);
    }

    @Override
    public Long getStorageTypeIdx() {
        return edgeId.getStorageTypeIdx();
    }

    @Override
    public byte[] getCachedId() {
        return id.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return edgeId.equals(o);
        final FireflyIdComposite that = (FireflyIdComposite) o;
        return java.util.Arrays.equals(id, that.id);
    }
}
