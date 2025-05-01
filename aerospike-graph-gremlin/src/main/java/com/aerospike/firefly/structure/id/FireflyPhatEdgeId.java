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

    private FireflyPhatEdgeId(final byte[] id, final long capacity, final String edgeSetName) {
        super(id, edgeSetName);
        this.capacity = capacity;
    }

    static FireflyPhatEdgeId fromByteBuffer(final ByteBuffer id, final long capacity, final String edgeSetName) {
        return fromByteArray(id.array(), capacity, edgeSetName);
    }

    static FireflyPhatEdgeId fromByteArray(final byte[] id, final long capacity, final String edgeSetName) {
        if (id.length != 16 && id.length != 8) {
            throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Provided id is not an 8 or 16 byte array.");
        }
        return new FireflyPhatEdgeId(id, capacity, edgeSetName);
    }

    static FireflyPhatEdgeId fromBase64String(final String id, final long capacity, final String edgeSetName) {
        byte[] decodedBytes = Base64.getDecoder().decode(id);
        try {
            return fromByteArray(decodedBytes, capacity, edgeSetName);
        } catch (final IllegalStateException e) {
            throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Base64 encoded String did not decode to a valid 8 or 16 byte array.");
        }
    }

    @Override
    protected void initialize(final Object id) {
        this.id = id;
    }

    public Long getPackingId() {
        if (this.packingId == null) {
            // Edge byte array is [<packingId>, <uniqueId (optional)>]
            final byte[] idBytes = ((byte[]) this.id);
            final byte[] bytes = Arrays.copyOfRange(idBytes, 0, 8);
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(bytes);
            buffer.flip();
            final long packingIdFromBuffer = buffer.getLong();
            this.packingId = packingIdFromBuffer;
            if (idBytes.length == 8) {
                this.uniqueId = packingIdFromBuffer;
            }
        }
        return this.packingId;
    }

    @Override
    public Object getUserId() {
        if (this.userId == null) {
            this.userId = Crypto.encodeBase64(((byte[]) this.id));
        }
        return this.userId;
    }

    @Override
    public Object getStorageId() {
        if (this.storageId == null) {
            this.storageId = Math.floorDiv(getPackingId(), capacity);
        }
        return this.storageId;
    }

    @Override
    public Long getUniqueId() {
        if (this.uniqueId == null) {
            final byte[] idBytes = (byte[]) this.id;
            final byte[] bytes;
            if (idBytes.length == 8) {
                bytes = Arrays.copyOfRange(idBytes, 0, 8);
            } else if (idBytes.length == 16) {
                bytes = Arrays.copyOfRange(idBytes, 8, 16);
            } else {
                // This should never happen
                throw new IllegalStateException("Edge ID was not of length 8 or 16. Please contact support.");
            }
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(bytes);
            buffer.flip();
            long uniqueIdFromBuffer = buffer.getLong();
            this.uniqueId = uniqueIdFromBuffer;
            if (idBytes.length == 8) {
                this.packingId = uniqueIdFromBuffer;
            }
        }
        return this.uniqueId;
    }

    @Override
    public ByteBuffer getEdgeIdBytes() {
        return ByteBuffer.wrap((byte[]) this.id);
    }

    /**
     * Whether this Edge ID is formed with a recycled packing ID, meaning the underlying ID byte array is a
     * 16-byte array, with 8 bytes for the packing ID and 8 bytes for the unique ID.
     * @return does this Edge ID contain a recycled packing ID
     */
    @Override
    public boolean isRecycled() {
        return ((byte[]) this.id).length != 8;
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
