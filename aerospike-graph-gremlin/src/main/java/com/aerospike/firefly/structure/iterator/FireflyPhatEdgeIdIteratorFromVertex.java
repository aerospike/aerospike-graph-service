package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeIdIteratorFromVertex extends FireflyPhatEdgeIdIterator {
    private final Direction direction;
    private final FireflyId vertexId;

    /**
     * Wrapper iterator for converting KeyRecord of Phat Edges into all of its contained edges' FireflyIds that are
     * attached to a specified Vertex.
     *
     * @param keyRecordIterator KeyRecord iterator to wrap.
     * @param db                AerospikeConnection instance.
     * @param direction         The Direction from the Vertex.
     * @param vertexId          The ID of the Vertex.
     */
    public FireflyPhatEdgeIdIteratorFromVertex(final Iterator<KeyRecord> keyRecordIterator,
                                               final AerospikeConnection db, final Direction direction,
                                               final FireflyId vertexId) {
        super(keyRecordIterator, db);
        this.direction = direction;
        this.vertexId = vertexId;
    }

    @Override
    protected void getNextKeyRecords() {
        final Set<ByteBuffer> edgeIds = new HashSet<>();
        final Record record = this.keyRecords.next().record;
        if (this.direction == Direction.BOTH || this.direction == Direction.OUT) {
            final String binName = getOutVBinName();
            final Map<ByteBuffer, String> edgeIdToOutVertexId = (Map<ByteBuffer, String>) record.getMap(binName);
            for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToOutVertexId.entrySet()) {
                if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashBase64())) {
                    edgeIds.add(edgeIdToVertexId.getKey());
                }
            }
        }
        if (this.direction == Direction.BOTH || this.direction == Direction.IN) {
            final String binName = getInVBinName();
            final Map<ByteBuffer, String> edgeIdToInVertexId = (Map<ByteBuffer, String>) record.getMap(binName);
            for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToInVertexId.entrySet()) {
                if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashBase64())) {
                    edgeIds.add(edgeIdToVertexId.getKey());
                }
            }
        }
        this.currentRecordEdgeIds = edgeIds.iterator();
    }

    protected String getInVBinName() {
        return Direction.IN.name();
    }

    protected String getOutVBinName() {
        return Direction.OUT.name();
    }
}
