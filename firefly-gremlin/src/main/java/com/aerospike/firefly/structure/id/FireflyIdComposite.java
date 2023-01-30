package com.aerospike.firefly.structure.id;

import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;

import java.nio.ByteBuffer;
import java.util.Optional;

import static com.aerospike.firefly.structure.id.FireflyIdFactory.IDX_TO_TYPE;
import static com.aerospike.firefly.structure.id.FireflyIdPoly.CONVERT_TO_USER_CLASS;

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
    private byte[] id;
    private final FireflyId adjacentId;
    private FireflyId edgeId;

    public FireflyIdComposite(final AerospikeConnection db, final FireflyId edgeId, final FireflyId adjacentId) {
        this.adjacentId = adjacentId;
        this.edgeId = edgeId;
        this.db = db;
        id = new byte[40];
        System.arraycopy(edgeId.getKeyHash(), 0, id, 0, 20);
        System.arraycopy(adjacentId.getKeyHash(), 0, id, 20, 20);
    }

    public FireflyIdComposite(final AerospikeConnection db, final byte[] ids) {
        if(ids.length != 40) throw new RuntimeException("Invalid id length");
        id = ids;
        edgeId = null;
        adjacentId = null;
        this.db = db;
    }


    /**
     * Slice the id at idx from our composite 2-hash array
     *
     * @param idx offset of 20 byte RIPEMD160 hash (Aerospike Key digest)
     * @return the 20 byte hash
     */
    private byte[] digestFromBytes(final int idx) {
        final byte[] digest = new byte[20];
        System.arraycopy(id, idx, digest, 0, 20);
        return digest;
    }

    /**
     * Get the edge id from the composite id
     *
     * @return edge id
     */
    public FireflyId getEdgeId() {
        if (edgeId != null) {
            return edgeId;
        }
        FireflyIdPoly idFromHash = FireflyIdPoly.fromHash(digestFromBytes(0), db.EDGE_AERO_SET);
        return idFromHash;
    }

    /**
     * Get the adjacent Vertex id from the composite id
     *
     * @return the id of vertex on other side of edge
     */
    public FireflyId getAdjacentId() {
        if (adjacentId != null) {
            return adjacentId;
        }
        return FireflyIdPoly.fromHash(digestFromBytes(20), db.VERTEX_AERO_SET);
    }

    /**
     * Get the original user id (user key) of the edge
     *
     * @return the user id of the edge
     */
    @Override
    public Object getUserId() {
        if (edgeId != null) {
            return edgeId.getUserId();
        }
        return Optional.ofNullable(FireflyRecord.read(db, db.EDGE_AERO_SET, FireflyIdPoly.fromHash(digestFromBytes(0), db.EDGE_AERO_SET)))
                .orElseThrow(() -> new RuntimeException("No original ID available and no Record present")).getUserKey();
    }

    @Override
    public Object getStorageId() {
        if (edgeId != null) {
            return edgeId.getStorageId();
        }
        FireflyIdPoly fid = FireflyIdPoly.fromHash(digestFromBytes(0), db.EDGE_AERO_SET);
        FireflyRecord x = FireflyRecord.read(db, db.EDGE_AERO_SET, fid );
        Optional<FireflyRecord> maybeRec = Optional.ofNullable(x);
        if (maybeRec.isEmpty()) return null;
        Long idx = maybeRec.get().record.getLong(db.IT_TYPE_BIN);
        Object userId = CONVERT_TO_USER_CLASS.get(IDX_TO_TYPE.get(idx)).getUserId(maybeRec.get().getUserKey());
        return userId;
    }

    @Override
    public Long getStorageTypeIdx() {
        if (edgeId != null) {
            return edgeId.getStorageTypeIdx();
        }
        edgeId = db.getIdFactory().createFromUser(FireflyEdge.class, digestFromBytes(0));
        return db.getIdFactory().createFromUser(FireflyEdge.class, edgeId).getStorageTypeIdx();
    }

    @Override
    public byte[] getCachedId() {
        if (id == null) {
            final ByteBuffer buffer = ByteBuffer.allocate(20 * 2);
            buffer.put(edgeId.getKeyHash());
            buffer.put(adjacentId.getKeyHash());
            id = buffer.array();
        }
        return id.clone();
    }

    @Override
    public byte[] getKeyHash() {
        return getEdgeId().getKeyHash();
    }

    @Override
    public String getKeyHashBase64() {
        return Crypto.encodeBase64(getKeyHash());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            if (edgeId != null) {
                return edgeId.equals(o);
            } else {
                edgeId = db.getIdFactory().createFromUser(FireflyEdge.class, digestFromBytes(0));
                return edgeId.equals(o);
            }
        }
        final FireflyIdComposite that = (FireflyIdComposite) o;
        return java.util.Arrays.equals(id, that.id);
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
