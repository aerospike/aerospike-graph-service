package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyEdge;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdComposite extends FireflyId {
    /* The composite id is used so heavily in different forms that
       the edge id, adjacent id, and id array are not always all needed
       but sometimes needed multiple times. Because of this, these are
       calculated lazily (and latched when needed the first time),
       to increase performance. */
    private byte[] id;
    private final FireflyId adjacentId;
    private FireflyId edgeId;

    public FireflyIdComposite(final FireflyId edgeId, final FireflyId adjacentId) {
        this.adjacentId = adjacentId;
        this.edgeId = edgeId;
    }

    public FireflyIdComposite(final byte[] ids) {
        id = ids;
        edgeId = null;
        adjacentId = null;
    }

    private long longFromBytes(final int idx) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(id, idx, Long.BYTES);
        buffer.flip();
        return buffer.getLong();
    }

    public FireflyId getEdgeId() {
        if (edgeId != null) {
            return edgeId;
        }
        return new FireflyIdNumeric(longFromBytes(0));
    }

    public FireflyId getAdjacentId() {
        if (adjacentId != null) {
            return adjacentId;
        }
        return FireflyIdFactory.createId(longFromBytes(Long.BYTES));
    }


    @Override
    public Object getUserId() {
        if (edgeId != null) {
            return edgeId.getUserId();
        }
        return FireflyIdFactory.createFromUser(FireflyEdge.class, longFromBytes(0)).getUserId();
    }

    @Override
    public Object getStorageId() {
        if (edgeId != null) {
            return edgeId.getStorageId();
        }
        return longFromBytes(0);
    }

    @Override
    public Long getStorageTypeIdx() {
        if (edgeId != null) {
            return edgeId.getStorageTypeIdx();
        }
        edgeId = FireflyIdFactory.createFromUser(FireflyEdge.class, longFromBytes(0));
        return FireflyIdFactory.createFromUser(FireflyEdge.class, longFromBytes(0)).getStorageTypeIdx();
    }

    @Override
    public byte[] getCachedId() {
        if (id == null) {
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            buffer.putLong((Long) edgeId.getStorageId());
            buffer.putLong((Long) adjacentId.getStorageId());
            id = buffer.array();
        }
        return id.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            if (edgeId != null) {
                return edgeId.equals(o);
            } else {
                edgeId = FireflyIdFactory.createFromUser(FireflyEdge.class, longFromBytes(0));
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
