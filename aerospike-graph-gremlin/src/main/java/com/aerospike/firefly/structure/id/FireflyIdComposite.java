/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.structure.id;

import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.aerospike.firefly.structure.id.RecyclingBufferedNumericIdManager.longToBytes;

public class FireflyIdComposite implements FireflyEdgeId {
    protected final AerospikeConnection db;
    /* The composite id is used so heavily in different forms that
       the edge id, adjacent id, and id array are not always all needed
       but sometimes needed multiple times. Because of this, these are
       calculated lazily (and latched when needed the first time),
       to increase performance. */
    protected List<Object> id; // 0: Vertex User ID; 1: Edge Packing ID; 2: Edge Unique ID (optional)
    protected FireflyId adjacentId;
    protected FireflyEdgeId edgeId;

    FireflyIdComposite(final AerospikeConnection db, final FireflyEdgeId edgeId, final FireflyId adjacentId) {
        this.db = db;
        this.adjacentId = adjacentId;
        this.edgeId = edgeId;
    }

    FireflyIdComposite(final AerospikeConnection db, final List<Object> idList) {
        if (idList == null || idList.size() < 2 || idList.size() > 3) {
            // This should never happen
            throw new IllegalArgumentException("Cannot create composite ID with list of unexpected size. Please contact support.");
        }
        this.db = db;
        this.id = idList;
    }

    /**
     * Get the edge id from the composite id
     *
     * @return edge id
     */
    public FireflyPhatEdgeId getEdgeId() {
        if (edgeId == null) {
            final int idSize = (this.id.size() - 1) * 8;
            final byte[] id = new byte[idSize];
            System.arraycopy(longToBytes((long) this.id.get(1)), 0, id, 0, 8);
            if (this.id.size() == 3) {
                System.arraycopy(longToBytes((long)this.id.get(2)), 0, id, 8, 8);
            }
            edgeId = FireflyPhatEdgeId.fromByteArray(id, db.getConfig().phatEdgeSize, db.getConfig().edgeAeroSet);
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
            adjacentId = db.getIdFactory().createVertexId(this.id.get(0));
        }
        return adjacentId;
    }

    public Object getAdjacentUserId() {
        if (this.id == null) {
            return this.adjacentId.getUserId();
        } else {
            return this.id.get(0);
        }
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
        if (this.id != null) {
            return (Long) this.id.get(1);
        } else {
            return this.getEdgeId().getPackingId();
        }
    }

    @Override
    public Long getUniqueId() {
        if (this.id != null) {
            return (Long) (this.id.size() == 3 ? this.id.get(2) : this.id.get(1));
        } else {
            return this.getEdgeId().getUniqueId();
        }
    }

    @Override
    public boolean isRecycled() {
        return this.getEdgeId().isRecycled();
    }

    @Override
    public List<Object> getCachedId() {
        if (this.id == null) {
            this.id = new ArrayList<>();
            this.id.add(0, this.adjacentId.getUserId());
            this.id.add(1, this.edgeId.getPackingId());
            if (this.edgeId.isRecycled()) {
                this.id.add(2, this.edgeId.getUniqueId());
            }
        }
        return List.copyOf(this.id);
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
        } else if (o instanceof FireflyIdComposite) {
            return this.getEdgeId().equals(((FireflyIdComposite) o).getEdgeId());
        } else {
            return this.getEdgeId().equals(o);
        }
    }

    @Override
    public String toString() {
        return "FireflyIdComposite{" +
                "adjacentId=" + getAdjacentUserId() +
                ", edgeId=" + getEdgeId() +
                '}';
    }
}
