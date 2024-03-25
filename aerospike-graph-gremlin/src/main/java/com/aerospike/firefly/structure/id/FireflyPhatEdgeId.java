package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeId extends FireflyIdPoly {
    private final long capacity;

    public FireflyPhatEdgeId(final ByteBuffer id, final long capacity, final String edgeSetName) {
        super(id, edgeSetName);
        this.capacity = capacity;
    }

    public long getPackingId() {
        // Edge byte array is [<recycledId>, <uniqueId>]
        // and the recycled id is used for the edge record.
        final byte[] bytes = Arrays.copyOfRange(((ByteBuffer)this.id).array(), 0, 8);
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    @Override
    public Object getStorageId() {
        return getPackingId() / capacity;
    }

    public byte getPackingIndex() {
        // This can be cast to a byte since capacity defaults to 10 and is capped at 100.
        return (byte) (getPackingId() % capacity);
    }

    /**
     * Uses the Aerospike Client Crypto routines to produce a RIPEMD160 hash of the string capable of retrieving the record by digest.
     *
     * @param setName the Aerospike namespace
     * @return the digest of the string
     */
    @Override
    public byte[] getIdHash(String setName) {
        final Value keyValue = Value.get(this.getStorageId());
        return Crypto.computeDigest(setName, keyValue);
    }

    @Override
    public int hashCode() {
        return getUserId().hashCode();
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof FireflyId) {
            Long userId = (Long) getUserId();
            return userId.compareTo((Long)((FireflyId) o).getUserId());
        } else {
            throw new IllegalArgumentException("Cannot compare FireflyId to " + o.getClass().getName());
        }
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && this.getUserId().equals(((FireflyId) o).getUserId());
    }
}
