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
    private Long storageId = null;
    private Long packingId = null;
    private Long uniqueId = null;
    private Integer hashcode = null;

    public FireflyPhatEdgeId(final ByteBuffer id, final long capacity, final String edgeSetName) {
        super(id, edgeSetName);
        this.capacity = capacity;
    }

    public Long getPackingId() {
        if (this.packingId == null) {
            // Edge byte array is [<recycledId>, <uniqueId>]
            final byte[] bytes = Arrays.copyOfRange(((ByteBuffer)this.id).array(), 0, 8);
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(bytes);
            buffer.flip();
            this.packingId = buffer.getLong();
        }
        return this.packingId;
    }

    @Override
    public Object getStorageId() {
        if (this.storageId == null) {
            this.storageId = getPackingId() / capacity;
        }
        return this.storageId;
    }

    public Long getUniqueId() {
        if (this.uniqueId == null) {
            // Edge byte array is [<recycledId>, <uniqueId>]
            final byte[] bytes = Arrays.copyOfRange(((ByteBuffer)this.id).array(), 8, 16);
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(bytes);
            buffer.flip();
            this.uniqueId = buffer.getLong();
        }
        return this.uniqueId;
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
        if (this.hashcode == null) {
            this.hashcode = getUserId().hashCode();
        }
        return this.hashcode;
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
    public boolean equals(final Object o) {
        return this.getUserId().equals(((FireflyId) o).getUserId()) && super.equals(o);
    }
}
