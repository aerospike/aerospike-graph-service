package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeIdIteratorFromIndexedVertex extends FireflyPhatEdgeIdIteratorFromVertex {
    /**
     * Wrapper iterator for converting KeyRecord of Phat Edges fetched via an Adjacency Index into all of its contained
     * edges' FireflyIds that are attached to a specified Vertex.
     *
     * @param keyRecordIterator KeyRecord iterator to wrap.
     * @param db                AerospikeConnection instance.
     * @param direction         The Direction from the Vertex.
     * @param vertexId          The ID of the Vertex.
     * @param labels            The labels of the Edge to filter on.
     */
    public FireflyPhatEdgeIdIteratorFromIndexedVertex(final Iterator<KeyRecord> keyRecordIterator,
                                                      final AerospikeConnection db,
                                                      final Direction direction,
                                                      final FireflyId vertexId,
                                                      final Set<String> labels,
                                                      final OutputType outputType) {
        super(keyRecordIterator, db, direction, vertexId, labels, outputType);
    }

    @Override
    protected Set<ByteBuffer> getIndividualEdgeIdsAttachedToVertex(final Record record, final Direction direction) {
        final String directionKey;
        if (direction == Direction.BOTH) {
            // Direction.BOTH should not be propagated here and should be combined at a higher level.
            throw new RuntimeException("Cannot get individual Edge IDs attached to a Vertex with Direction.BOTH");
        } else {
            directionKey = direction == Direction.OUT ? db.SUPERNODES_OUT_BIN : db.SUPERNODES_IN_BIN;
        }

        final Map<ByteBuffer, String> edgeIdToVertexIdMap = (Map<ByteBuffer, String>) record.getMap(directionKey);
        if (edgeIdToVertexIdMap == null) {
            return Collections.emptySet();
        }

        final Set<ByteBuffer> attachedEdgeIds = new HashSet<>();
        for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToVertexIdMap.entrySet()) {
            if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashString())) {
                attachedEdgeIds.add(edgeIdToVertexId.getKey());
            }
        }
        return attachedEdgeIds;
    }
}
