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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
        final List<Object> outputIds = new ArrayList<>();

        final Record record = this.keyRecords.next().record;
        final Map<ByteBuffer, String> edgeIdToEdgeLabel = (Map<ByteBuffer, String>) record.getMap(db.LABEL_BIN);

        if (this.direction == Direction.BOTH || this.direction == Direction.OUT) {
            final Map<ByteBuffer, String> edgeIdToOutVertexId = (Map<ByteBuffer, String>) record.getMap(getOutVBinName());
            final Map<ByteBuffer, String> combinedEdgeIdToOutVertexId = (Map<ByteBuffer, String>) record.getMap(getInVBinName());
            combinedEdgeIdToOutVertexId.putAll((Map<ByteBuffer, String>) record.getMap(Direction.IN.name()));
            for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToOutVertexId.entrySet()) {
                if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashBase64()) &&
                        (labels.isEmpty() || labels.contains(edgeIdToEdgeLabel.get(edgeIdToVertexId.getKey())))) {
                    if (outputType == OutputType.VERTEX_ID) {
                        outputIds.add(combinedEdgeIdToOutVertexId.get(edgeIdToVertexId.getKey()));
                    } else {
                        outputIds.add(edgeIdToVertexId.getKey());
                    }
                }
            }
        }

        if (this.direction == Direction.BOTH || this.direction == Direction.IN) {
            final Map<ByteBuffer, String> edgeIdToInVertexId = (Map<ByteBuffer, String>) record.getMap(getInVBinName());
            final Map<ByteBuffer, String> combinedEdgeIdToInVertexId = (Map<ByteBuffer, String>) record.getMap(getOutVBinName());
            combinedEdgeIdToInVertexId.putAll((Map<ByteBuffer, String>) record.getMap(Direction.OUT.name()));
            for (final Map.Entry<ByteBuffer, String> edgeIdToVertexId : edgeIdToInVertexId.entrySet()) {
                if (edgeIdToVertexId.getValue().equals(this.vertexId.getKeyHashBase64()) &&
                        (labels.isEmpty() || labels.contains(edgeIdToEdgeLabel.get(edgeIdToVertexId.getKey())))) {
                    if (outputType == OutputType.VERTEX_ID) {
                        outputIds.add(combinedEdgeIdToInVertexId.get(edgeIdToVertexId.getKey()));
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
            final Object element = this.currentRecordIds.next();
            if (element == null) {
                System.out.println("null excep");
                throw FastNoSuchElementException.instance();
            }
            if (outputType.equals(OutputType.VERTEX_ID)) {
                return FireflyIdPoly.fromBase64Hash((String) element, db.VERTEX_AERO_SET);
            } else {
                return new FireflyPhatEdgeId((ByteBuffer) element, this.db.PHAT_EDGE_SIZE, this.db.EDGE_AERO_SET);
            }
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
