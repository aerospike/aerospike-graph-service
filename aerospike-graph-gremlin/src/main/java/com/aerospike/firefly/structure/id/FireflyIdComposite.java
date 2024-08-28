package com.aerospike.firefly.structure.id;

import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdComposite extends FireflyId {
    private final AerospikeConnection db;
    /* The composite id is used so heavily in different forms that
       the edge id, adjacent id, and id array are not always all needed
       but sometimes needed multiple times. Because of this, these are
       calculated lazily (and latched when needed the first time),
       to increase performance. */
    private final byte[] id;
    int idSize;
    private FireflyId adjacentId;
    private FireflyId edgeId;
    private Object adjacentUserId;
    private Byte adjacentIdTypeHint;

    public FireflyIdComposite(final AerospikeConnection db, final FireflyId edgeId, final FireflyId adjacentId) {
        this.db = db;
        this.adjacentId = adjacentId;
        this.edgeId = edgeId;
        byte[] encodedAdjacentUserId = null;
        if (this.db.ENABLE_CACHED_ADJACENT_ID_STRATEGY) {
            this.adjacentUserId = adjacentId.getUserId();
            this.adjacentIdTypeHint = (byte) db.getIdFactory().getTypeHint(this.adjacentUserId);
            encodedAdjacentUserId = encodeIdToBytes(this.adjacentUserId);
            // 37 to account for the type hint byte.
            this.idSize = 37 + encodedAdjacentUserId.length;
        } else {
            this.idSize = 36;
        }
        this.id = new byte[this.idSize];
        System.arraycopy(((ByteBuffer) edgeId.getUserId()).array(), 0, this.id, 0, 16);
        System.arraycopy(adjacentId.getKeyHash(), 0, this.id, 16, 20);
        if (this.db.ENABLE_CACHED_ADJACENT_ID_STRATEGY) {
            this.id[36] = adjacentIdTypeHint;
            System.arraycopy(encodedAdjacentUserId, 0, this.id, 37, encodedAdjacentUserId.length);
        }
    }

    public FireflyIdComposite(final AerospikeConnection db, final byte[] id) {
        if (id.length != 36 && !db.ENABLE_CACHED_ADJACENT_ID_STRATEGY) {
            throw new RuntimeException("Invalid id length of " + id.length + " for composite id. Length should be 36.");
        }
        this.db = db;
        this.id = id;
        this.idSize = id.length;
        this.adjacentId = null;
        this.edgeId = null;
        this.adjacentUserId = null;
        if (db.ENABLE_CACHED_ADJACENT_ID_STRATEGY) {
            this.adjacentIdTypeHint = this.id[36];
        }
    }

    /**
     * Slice the id at offset from our composite 2-hash array
     *
     * @param offset offset of 20 byte RIPEMD160 hash (Aerospike Key digest)
     * @return the 20 byte hash
     */
    private byte[] digestFromBytes(final int offset) {
        final byte[] digest = new byte[20];
        System.arraycopy(id, offset, digest, 0, 20);
        return digest;
    }

    /**
     * Get the edge id from the composite id
     *
     * @return edge id
     */
    public FireflyPhatEdgeId getEdgeId() {
        if (edgeId == null) {
            final byte[] individualEdgeId = new byte[16];
            System.arraycopy(id, 0, individualEdgeId, 0, 16);
            edgeId = new FireflyPhatEdgeId(ByteBuffer.wrap(individualEdgeId), db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
        }
        return (FireflyPhatEdgeId) edgeId;
    }

    /**
     * Get the adjacent Vertex id from the composite id
     *
     * @return the id of vertex on other side of edge
     */
    public FireflyId getAdjacentId() {
        if (adjacentId == null) {
            adjacentId = FireflyIdPoly.fromHash(digestFromBytes(16), db.VERTEX_AERO_SET);
        }
        return adjacentId;
    }

    public Object getAdjacentUserId() {

    }

    /**
     * Get the original user id (user key) of the edge
     *
     * @return the user id of the edge
     */
    @Override
    public Object getUserId() {
        return this.getEdgeId().getUserId();
    }

    @Override
    public Object getStorageId() {
        return this.getEdgeId().getStorageId();
    }

    @Override
    public Long getStorageTypeHint() {
        return this.getEdgeId().getStorageTypeHint();
    }

    @Override
    public byte[] getCachedId() {
        return this.id.clone();
    }

    @Override
    public byte[] getKeyHash() {
        return getEdgeId().getKeyHash();
    }

    @Override
    public String getKeyHashString() {
        return new String(getKeyHash(), StandardCharsets.ISO_8859_1);
    }

    @Override
    public String getKeyHashBase64() {
        return Crypto.encodeBase64(getKeyHash());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (this.getClass() != o.getClass()) {
            return this.getEdgeId().equals(o);
        }
        final FireflyIdComposite that = (FireflyIdComposite) o;
        return java.util.Arrays.equals(this.id, that.id);
    }

    @Override
    public String toString() {
        return "FireflyIdComposite{" +
                "id=" + java.util.Arrays.toString(id) +
                ", adjacentId=" + getAdjacentId() +
                ", edgeId=" + getEdgeId() +
                '}';
    }

    private byte[] encodeIdToBytes(final Object id) {
        if (id instanceof byte[]) {
            return (byte[]) id;
        } else if (id instanceof Double) {
            return ByteBuffer.allocate(8).putDouble((Double) id).array();
        } else if (id instanceof Long) {
            return ByteBuffer.allocate(8).putLong((Long) id).array();
        } else if (id instanceof Integer) {
            return ByteBuffer.allocate(4).putInt((Integer) id).array();
        } else if (id instanceof String) {
            return ((String) id).getBytes(StandardCharsets.ISO_8859_1);
        } else {
            // This should never happen unless we support additional id types but forget to update this function.
            throw new RuntimeException("Invalid adjacent vertex user ID type for composite id. Found type: " +
                    id.getClass());
        }
    }
}
