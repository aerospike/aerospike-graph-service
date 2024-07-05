package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_IN_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_OUT_KEY;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeIdIteratorFromIndexedVertex extends FireflyPhatEdgeIdIteratorFromVertex {
    private final FireflyId adjacentVertexId;

    /**
     * Wrapper iterator for converting KeyRecord of Phat Edges fetched via an Adjacency Index into all of its contained
     * edges' FireflyIds that are attached to a specified Vertex.
     *
     * @param keyRecordIterator KeyRecord iterator to wrap.
     * @param db                AerospikeConnection instance.
     * @param direction         The Direction from the Vertex.
     * @param vertexId          The ID of the Vertex.
     * @param labels            The labels of the Edge to filter on.
     * @param adjacentVertexId  The ID of the adjacent Vertex to filter with. Can be null.
     */
    public FireflyPhatEdgeIdIteratorFromIndexedVertex(final Iterator<KeyRecord> keyRecordIterator,
                                                      final AerospikeConnection db,
                                                      final Direction direction,
                                                      final FireflyId vertexId,
                                                      final Set<String> labels,
                                                      final OutputType outputType,
                                                      final FireflyId adjacentVertexId) {
        super(keyRecordIterator, db, direction, vertexId, labels, outputType);
        this.adjacentVertexId = adjacentVertexId;
    }

    @Override
    protected Set<ByteBuffer> getIndividualEdgeIdsAttachedToVertex(final Record record, final Direction direction) {
        final String directionKey;
        final String adjacentVertexMapKey;
        if (direction == Direction.BOTH) {
            // Direction.BOTH should not be propagated here and should be combined at a higher level.
            throw new RuntimeException("Cannot get individual Edge IDs attached to a Vertex with Direction.BOTH");
        } else {
            if (direction == Direction.OUT) {
                directionKey = db.SUPERNODES_OUT_BIN;
                adjacentVertexMapKey = EDGE_SUPERNODE_IN_KEY;
            } else {
                directionKey = db.SUPERNODES_IN_BIN;
                adjacentVertexMapKey = EDGE_SUPERNODE_OUT_KEY;
            }
        }

        final Map<ByteBuffer, String> edgeIdToVertexIdMap = (Map<ByteBuffer, String>) record.getMap(directionKey);
        if (edgeIdToVertexIdMap == null) {
            return Collections.emptySet();
        }

        final Set<Long> uniqueEdgeIdsAttachedToAdjacentVertex = new HashSet<>();
        if (adjacentVertexId != null) {
            final Map<String, Object> adjacencyPushdowns = (Map<String, Object>) record.getMap(db.SUPERNODE_EDGE_PROPERTIES_BIN);
            if (adjacencyPushdowns != null) {
                final Map<String, Object> propertyKeys = (Map<String, Object>) adjacencyPushdowns.get(vertexId.getKeyHashString());
                if (propertyKeys != null) {
                    final Map<Long, String> edgeIdToVertexId = (Map<Long, String>) propertyKeys.get(adjacentVertexMapKey);
                    if (edgeIdToVertexId != null) {
                        for (final Map.Entry<Long, String> edgeIdToVertexIdEntry : edgeIdToVertexId.entrySet()) {
                            if (edgeIdToVertexIdEntry.getValue().equals(adjacentVertexId.getKeyHashString())) {
                                uniqueEdgeIdsAttachedToAdjacentVertex.add(edgeIdToVertexIdEntry.getKey());
                            }
                        }
                    }
                }
            }
        }

        final Set<ByteBuffer> attachedEdgeIds = new HashSet<>();
        for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToVertexIdMap.entrySet()) {
            if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashString())) {
                if (adjacentVertexId == null) {
                    attachedEdgeIds.add(edgeIdToVertexId.getKey());
                } else {
                    final FireflyPhatEdgeId edgeId = new FireflyPhatEdgeId(edgeIdToVertexId.getKey(), this.db.PHAT_EDGE_SIZE, this.db.EDGE_AERO_SET);
                    if (uniqueEdgeIdsAttachedToAdjacentVertex.contains(edgeId.getUniqueId())) {
                        attachedEdgeIds.add(edgeIdToVertexId.getKey());
                    }
                }
            }
        }
        return attachedEdgeIds;
    }
}
