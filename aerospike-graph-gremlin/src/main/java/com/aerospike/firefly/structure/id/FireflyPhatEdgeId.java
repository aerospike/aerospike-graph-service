package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeId extends FireflyIdPoly implements FireflyEdgeId {
    private final long capacity;
    private Long storageId = null;
    private Long packingId = null;
    private Long uniqueId = null;
    private Integer hashcode = null;

    private FireflyPhatEdgeId(final ByteBuffer id, final long capacity, final String edgeSetName) {
        super(id, edgeSetName);
        this.capacity = capacity;
    }

    static FireflyPhatEdgeId fromByteBuffer(final ByteBuffer id, final long capacity, final String edgeSetName) {
        return new FireflyPhatEdgeId(id, capacity, edgeSetName);
    }

    static FireflyPhatEdgeId fromByteArray(final byte[] id, final long capacity, final String edgeSetName) {
        if (id.length != 16) {
            throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Provided id is not a 16 byte array.");
        }
        return fromByteBuffer(ByteBuffer.wrap(id), capacity, edgeSetName);
    }

    static FireflyPhatEdgeId fromBase64String(final String id, final long capacity, final String edgeSetName) {
        try{
            final byte[] decodedBytes = Base64.getDecoder().decode(id);
            return fromByteArray(decodedBytes, capacity, edgeSetName);
        } catch(final IllegalArgumentException | IllegalStateException e) {
            throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Base64 encoded String did not decode to a valid 16 byte array.");
        }
    }

    @Override
    protected void initialize(final Object id) {
        this.id = id;
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
    public Object getUserId() {
        if (this.userId == null) {
            this.userId = Crypto.encodeBase64(((ByteBuffer)this.id).array());
        }
        return this.userId;
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

    @Override
    public ByteBuffer getEdgeIdBytes() {
        return (ByteBuffer)this.id;
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
            this.hashcode = getEdgeIdBytes().hashCode();
        }
        return this.hashcode;
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof FireflyId) {
            return this.getUserId().equals(((FireflyId) o).getUserId()) && super.equals(o);
        } else{
            return false;
        }
    }
}
