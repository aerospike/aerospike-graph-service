package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public abstract class FireflyPhatEdgeIdIteratorFromVertex extends FireflyPhatEdgeIdIterator {
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

        while (outputIds.isEmpty() && this.keyRecords.hasNext()) {
            final Record record = this.keyRecords.next().record;
            final Map<ByteBuffer, List<?>> edgeIdToData = (Map<ByteBuffer, List<?>>) record.getMap(db.EDGE_DATA_BIN);

            if (this.direction == Direction.OUT || this.direction == Direction.BOTH) {
                final Set<ByteBuffer> outEdgeIds = getIndividualEdgeIdsAttachedToVertex(record, Direction.OUT);
                for (final ByteBuffer edgeId : outEdgeIds) {
                    if (labels.isEmpty() || labels.contains((String) edgeIdToData.get(edgeId).get(LABEL_POSITION))) {
                        if (outputType == OutputType.VERTEX_ID) {
                            final Object vertexId = edgeIdToData.get(edgeId).get(IN_V_POSITION);
                            outputIds.add(vertexId);
                        } else {
                            outputIds.add(edgeId);
                        }
                    }
                }
            }

            if (this.direction == Direction.IN || this.direction == Direction.BOTH) {
                final Set<ByteBuffer> inEdgeIds = getIndividualEdgeIdsAttachedToVertex(record, Direction.IN);
                for (final ByteBuffer edgeId : inEdgeIds) {
                    if (labels.isEmpty() || labels.contains((String) edgeIdToData.get(edgeId).get(LABEL_POSITION))) {
                        if (outputType == OutputType.VERTEX_ID) {
                            final Object vertexId = edgeIdToData.get(edgeId).get(OUT_V_POSITION);
                            outputIds.add(vertexId);
                        } else {
                            outputIds.add(edgeId);
                        }
                    }
                }
            }
        }

        this.currentRecordIds = outputIds.iterator();
    }

    // Make this not abstract if we ever need to use this class in the future
    protected abstract Set<ByteBuffer> getIndividualEdgeIdsAttachedToVertex(final Record record, final Direction direction);

    @Override
    public FireflyId next() {
        if (hasNext()) {
            final Object element = this.currentRecordIds.next();
            if (element == null) {
                throw FastNoSuchElementException.instance();
            }
            if (outputType.equals(OutputType.VERTEX_ID)) {
                return this.db.getIdFactory().createVertexId(element);
            } else {
                return this.db.getIdFactory().createEdgeId(element);
            }
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
