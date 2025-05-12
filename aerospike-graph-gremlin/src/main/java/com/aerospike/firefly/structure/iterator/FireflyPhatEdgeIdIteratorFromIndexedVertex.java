package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_IN_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_OUT_KEY;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeIdIteratorFromIndexedVertex extends FireflyPhatEdgeIdIteratorFromVertex {
    private final FireflyId adjacentVertexId;
    private final Map<FireflyId, FireflyEdge> edgeCache;
    private final FireflyGraph graph;

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
        this.edgeCache = null;
        this.graph = null;
    }

    public FireflyPhatEdgeIdIteratorFromIndexedVertex(final Iterator<KeyRecord> keyRecordIterator,
                                                      final FireflyGraph graph,
                                                      final Direction direction,
                                                      final FireflyId vertexId,
                                                      final Set<String> labels,
                                                      final OutputType outputType,
                                                      final FireflyId adjacentVertexId,
                                                      final Map<FireflyId, FireflyEdge> edgeCache) {
        super(keyRecordIterator, graph.getBaseGraph(), direction, vertexId, labels, outputType);
        this.adjacentVertexId = adjacentVertexId;
        this.edgeCache = edgeCache;
        this.graph = graph;
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

        final Map<Long, String> edgeUniqueIdToVertexIdMap = (Map<Long, String>) record.getMap(directionKey);
        if (edgeUniqueIdToVertexIdMap == null) {
            return Collections.emptySet();
        }

        final Set<Long> uniqueEdgeIdsAttachedToAdjacentVertex = new HashSet<>();
        if (adjacentVertexId != null) {
            final Map<Object, Object> adjacencyPushdowns = (Map<Object, Object>) record.getMap(db.SUPERNODE_EDGE_PROPERTIES_BIN);
            if (adjacencyPushdowns != null) {
                final Map<String, Object> propertyKeys = (Map<String, Object>) adjacencyPushdowns.get(vertexId.getUserId());
                if (propertyKeys != null) {
                    final Map<Long, Object> edgeIdToVertexId = (Map<Long, Object>) propertyKeys.get(adjacentVertexMapKey);
                    if (edgeIdToVertexId != null) {
                        for (final Map.Entry<Long, Object> edgeUniqueIdToVertexIdEntry : edgeIdToVertexId.entrySet()) {
                            if (edgeUniqueIdToVertexIdEntry.getValue().equals(adjacentVertexId.getUserId())) {
                                uniqueEdgeIdsAttachedToAdjacentVertex.add(edgeUniqueIdToVertexIdEntry.getKey());
                            }
                        }
                    }
                }
            }
        }

        final Map<ByteBuffer, List<?>> edgeIdToData = (Map<ByteBuffer, List<?>>) record.getMap(db.EDGE_DATA_BIN);
        final Map<Long, ByteBuffer> uniqueIdToByteId = new HashMap<>();
        for (final ByteBuffer byteId : edgeIdToData.keySet()) {
            final long uniqueId = this.db.getIdFactory().createEdgeId(byteId).getUniqueId();
            uniqueIdToByteId.put(uniqueId, byteId);
        }

        final Set<ByteBuffer> attachedEdgeIds = new HashSet<>();
        for (final Map.Entry<Long, String> edgeUniqueIdToVertexId : edgeUniqueIdToVertexIdMap.entrySet()) {
            if (edgeUniqueIdToVertexId.getValue().equals(this.vertexId.getKeyHashString())) {
                if (adjacentVertexId == null) {
                    attachedEdgeIds.add(uniqueIdToByteId.get(edgeUniqueIdToVertexId.getKey()));
                } else {
                    if (uniqueEdgeIdsAttachedToAdjacentVertex.contains(edgeUniqueIdToVertexId.getKey())) {
                        attachedEdgeIds.add(uniqueIdToByteId.get(edgeUniqueIdToVertexId.getKey()));
                    }
                }
            }
        }
        if (edgeCache != null) {
            for (final ByteBuffer edgeId : attachedEdgeIds) {
                final FireflyId dbEdgeId = db.getIdFactory().createEdgeId(edgeId);
                edgeCache.put(dbEdgeId, FireflyEdgeFactory.create(dbEdgeId, record, graph));
            }
        }
        return attachedEdgeIds;
    }
}
