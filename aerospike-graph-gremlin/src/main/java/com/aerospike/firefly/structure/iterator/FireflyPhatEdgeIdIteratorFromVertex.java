package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
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
    protected final Direction direction;
    protected final FireflyId vertexId;
    protected final Set<String> labels;
    protected final OutputType outputType;

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
                                               final AerospikeConnection db,
                                               final Direction direction,
                                               final FireflyId vertexId,
                                               final Set<String> labels,
                                               final OutputType outputType) {
        super(keyRecordIterator, db);
        this.direction = direction;
        this.vertexId = vertexId;
        this.labels = labels;
        this.outputType = outputType;
    }

    public enum OutputType {
        VERTEX_ID,
        EDGE_ID
    }

    @Override
    protected void getNextKeyRecords() {
        final Set<Object> outputIds = new HashSet<>();

        final Record record = this.keyRecords.next().record;
        final Map<ByteBuffer, String> edgeIdToEdgeLabel = (Map<ByteBuffer, String>) record.getMap(db.LABEL_BIN);
        Map<ByteBuffer, String> edgeIdToOutVertexId = (Map<ByteBuffer, String>) record.getMap(getOutVBinName());
        Map<ByteBuffer, String> edgeIdToInVertexId = (Map<ByteBuffer, String>) record.getMap(getInVBinName());
        if (edgeIdToInVertexId == null) {
            edgeIdToInVertexId = (Map<ByteBuffer, String>) record.getMap(Direction.IN.name());
        }
        if (edgeIdToOutVertexId == null) {
            edgeIdToOutVertexId = (Map<ByteBuffer, String>) record.getMap(Direction.OUT.name());
        }

        if (this.direction == Direction.BOTH || this.direction == Direction.OUT) {
            for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToOutVertexId.entrySet()) {
                if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashBase64()) &&
                        (labels.isEmpty() || labels.contains(edgeIdToEdgeLabel.get(edgeIdToVertexId.getKey())))) {
                    if (outputType == OutputType.VERTEX_ID) {
                        outputIds.add(edgeIdToInVertexId.get(edgeIdToVertexId.getKey()));
                    } else {
                        outputIds.add(edgeIdToVertexId.getKey());
                    }
                }
            }
        }

        if (this.direction == Direction.BOTH || this.direction == Direction.IN) {
            for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToInVertexId.entrySet()) {
                if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashBase64()) &&
                        (labels.isEmpty() || labels.contains(edgeIdToEdgeLabel.get(edgeIdToVertexId.getKey())))) {
                    if (outputType == OutputType.VERTEX_ID) {
                        outputIds.add(edgeIdToInVertexId.get(edgeIdToVertexId.getKey()));
                    } else {
                        outputIds.add(edgeIdToVertexId.getKey());
                    }
                }
            }
        }

        this.currentRecordIds = outputIds.iterator();
    }

    protected String getInVBinName() {
        return Direction.IN.name();
    }

    protected String getOutVBinName() {
        return Direction.OUT.name();
    }

    @Override
    public FireflyId next() {
        if (hasNext()) {
            if (outputType.equals(OutputType.VERTEX_ID)) {
                return FireflyIdPoly.fromBase64Hash((String) this.currentRecordIds.next(), db.VERTEX_AERO_SET);
            } else {
                return new FireflyPhatEdgeId((ByteBuffer) this.currentRecordIds.next(), this.db.PHAT_EDGE_SIZE, this.db.EDGE_AERO_SET);
            }
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
