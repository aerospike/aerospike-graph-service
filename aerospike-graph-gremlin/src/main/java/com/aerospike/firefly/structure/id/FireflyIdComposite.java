package com.aerospike.firefly.structure.id;

import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdComposite implements FireflyEdgeId {
    protected final AerospikeConnection db;
    /* The composite id is used so heavily in different forms that
       the edge id, adjacent id, and id array are not always all needed
       but sometimes needed multiple times. Because of this, these are
       calculated lazily (and latched when needed the first time),
       to increase performance. */
    protected byte[] id;
    protected FireflyId adjacentId;
    protected FireflyEdgeId edgeId;

    protected FireflyIdComposite(final AerospikeConnection db, final FireflyEdgeId edgeId, final FireflyId adjacentId) {
        this.db = db;
        initialize(edgeId, adjacentId);
    }

    protected void initialize(final FireflyEdgeId edgeId, final FireflyId adjacentId) {
        this.adjacentId = adjacentId;
        this.edgeId = edgeId;
        this.id = new byte[36];
        System.arraycopy(edgeId.getEdgeIdBytes().array(), 0, this.id, 0, 16);
        System.arraycopy(adjacentId.getKeyHash(), 0, this.id, 16, 20);
    }

    protected FireflyIdComposite(final AerospikeConnection db, final byte[] id) {
        this.db = db;
        initialize(id);
    }

    protected void initialize(final byte[] id) {
        if (id.length != 36) {
            throw new RuntimeException("Invalid id length of " + id.length + " for composite id. Length should be 36.");
        }
        this.id = id;
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
            edgeId = FireflyPhatEdgeId.fromByteArray(individualEdgeId, db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
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
            adjacentId = db.getIdFactory().createVertexIdFromHash(digestFromBytes(16));
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
        return this.getEdgeId().getUserId();
    }

    @Override
    public Object getStorageId() {
        return this.getEdgeId().getStorageId();
    }

    @Override
    public ByteBuffer getEdgeIdBytes() {
        return this.getEdgeId().getEdgeIdBytes();
    }

    @Override
    public Long getStorageTypeHint() {
        return this.getEdgeId().getStorageTypeHint();
    }

    @Override
    public Long getPackingId() {
        return this.getEdgeId().getPackingId();
    }

    @Override
    public Long getUniqueId() {
        return this.getEdgeId().getUniqueId();
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
    public int hashCode() {
        return this.getKeyHash() == null ? this.getStorageId().hashCode() : Arrays.hashCode(this.getKeyHash());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FireflyUserIdComposite) {
            return this.getEdgeId().equals(((FireflyUserIdComposite) o).getEdgeId());
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
}
