package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyVertex;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FireflyUserIdComposite extends FireflyIdComposite {
    private Object adjacentUserId;
    private Byte adjacentIdTypeHint;
    private int idSize;

    public FireflyUserIdComposite(final AerospikeConnection db, final FireflyId edgeId, final FireflyId adjacentId) {
        super(db, edgeId, adjacentId);
    }

    @Override
    protected void initialize(final FireflyId edgeId, final FireflyId adjacentId) {
        this.adjacentUserId = adjacentId.getUserId();
        this.adjacentIdTypeHint = (byte) this.db.getIdFactory().getTypeHint(this.adjacentUserId);
        final byte[] encodedAdjacentUserId = encodeIdToBytes(this.adjacentUserId);
        // 17 comes from edge id (16) and type hint byte (1).
        this.idSize = 17 + encodedAdjacentUserId.length;
        this.id = new byte[this.idSize];
        System.arraycopy(((ByteBuffer) edgeId.getUserId()).array(), 0, this.id, 0, 16);
        this.id[16] = this.adjacentIdTypeHint;
        System.arraycopy(encodedAdjacentUserId, 0, this.id, 17, encodedAdjacentUserId.length);
    }

    public FireflyUserIdComposite(final AerospikeConnection db, final byte[] id) {
        super(db, id);
    }

    @Override
    protected void initialize(final byte[] id) {
        this.id = id;
        this.adjacentIdTypeHint = this.id[16];
        this.idSize = id.length;
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

    private Object decodeBytesToUserId(final byte[] bytes) {
        final long typeHint = (long) adjacentIdTypeHint;
        final FireflyIdFactory idFactory = this.db.getIdFactory();
        if (typeHint == idFactory.getTypeHint(byte[].class)) {
            return bytes;
        } else if (typeHint == idFactory.getTypeHint(String.class)) {
            return new String(bytes, StandardCharsets.ISO_8859_1);
        } else if (typeHint == idFactory.getTypeHint(Long.class)) {
            return ByteBuffer.wrap(bytes).getLong();
        } else if (typeHint == idFactory.getTypeHint(Integer.class)) {
            return ByteBuffer.wrap(bytes).getInt();
        } else if (typeHint == idFactory.getTypeHint(Double.class)) {
            return ByteBuffer.wrap(bytes).getDouble();
        } else {
            // This should never happen.
            throw new RuntimeException("Invalid type hint for user id: " + typeHint);
        }
    }

    @Override
    public FireflyId getAdjacentId() {
        if (this.adjacentId == null) {
            this.adjacentId = this.db.getIdFactory().createId(getAdjacentUserId(), FireflyVertex.class);
        }
        return this.adjacentId;
    }

    public Object getAdjacentUserId() {
        if (this.adjacentUserId == null) {
            final int adjacentIdSize = this.idSize - 17;
            final byte[] encodedAdjacentUserId = new byte[adjacentIdSize];
            System.arraycopy(this.id, 17, encodedAdjacentUserId, 0, adjacentIdSize);
            this.adjacentUserId = decodeBytesToUserId(encodedAdjacentUserId);
        }
        return this.adjacentUserId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FireflyUserIdComposite) {
            return java.util.Arrays.equals(this.id, ((FireflyUserIdComposite) o).id);
        }
        if (o instanceof FireflyIdComposite) {
            return this.getEdgeId().equals(((FireflyIdComposite) o).getEdgeId());
        } else {
            return this.getEdgeId().equals(o);
        }
    }
}
