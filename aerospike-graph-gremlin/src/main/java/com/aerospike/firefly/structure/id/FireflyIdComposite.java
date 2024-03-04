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
    private FireflyId adjacentId;
    private final FireflyId edgeId;

    public FireflyIdComposite(final AerospikeConnection db, final FireflyId edgeId, final FireflyId adjacentId) {
        this.adjacentId = adjacentId;
        this.edgeId = edgeId;
        this.db = db;
        this.id = new byte[36];
        System.arraycopy(((ByteBuffer) edgeId.getUserId()).array(), 0, this.id, 0, 16);
        System.arraycopy(adjacentId.getKeyHash(), 0, this.id, 16, 20);
    }

    public FireflyIdComposite(final AerospikeConnection db, final byte[] id) {
        if (id.length != 36) {
            throw new RuntimeException("Invalid id length of " + id.length + " for composite id. Length should be 36.");
        }
        this.id = id;
        final byte[] individualEdgeId = new byte[16];
        System.arraycopy(id, 0, individualEdgeId, 0, 16);
        this.edgeId = new FireflyPhatEdgeId(ByteBuffer.wrap(individualEdgeId), db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
        this.adjacentId = null;
        this.db = db;
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
    public FireflyId getEdgeId() {
        return this.edgeId;
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

    /**
     * Get the original user id (user key) of the edge
     *
     * @return the user id of the edge
     */
    @Override
    public Object getUserId() {
        return this.edgeId.getUserId();
    }

    @Override
    public Object getStorageId() {
        return this.edgeId.getStorageId();
    }

    @Override
    public Long getStorageTypeHint() {
        return this.edgeId.getStorageTypeHint();
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
            return this.edgeId.equals(o);
        }
        final FireflyIdComposite that = (FireflyIdComposite) o;
        return java.util.Arrays.equals(this.id, that.id);
    }

    @Override
    public String toString() {
        return "FireflyIdComposite{" +
                "id=" + java.util.Arrays.toString(id) +
                ", adjacentId=" + adjacentId +
                ", edgeId=" + edgeId +
                '}';
    }
}
