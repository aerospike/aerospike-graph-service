package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
            final FireflyEdgeRecord edgeRecord = new FireflyEdgeRecord(record, db);

            if (this.direction == Direction.OUT || this.direction == Direction.BOTH) {
                final Set<ByteBuffer> outEdgeIds = getIndividualEdgeIdsAttachedToVertex(record, Direction.OUT);
                for (final ByteBuffer edgeIdBytes : outEdgeIds) {
                    final FireflyEdgeId edgeId = this.db.getIdFactory().createEdgeId(edgeIdBytes);
                    if (labels.isEmpty() || labels.contains(edgeRecord.getLabel(edgeId.getUniqueId()))) {
                        if (outputType == OutputType.VERTEX_ID) {
                            final FireflyId vertexId = edgeRecord.getInV(edgeId.getUniqueId());
                            outputIds.add(vertexId);
                        } else {
                            outputIds.add(edgeId);
                        }
                    }
                }
            }

            if (this.direction == Direction.IN || this.direction == Direction.BOTH) {
                final Set<ByteBuffer> inEdgeIds = getIndividualEdgeIdsAttachedToVertex(record, Direction.IN);
                for (final ByteBuffer edgeIdBytes : inEdgeIds) {
                    final FireflyEdgeId edgeId = this.db.getIdFactory().createEdgeId(edgeIdBytes);
                    if (labels.isEmpty() || labels.contains(edgeRecord.getLabel(edgeId.getUniqueId()))) {
                        if (outputType == OutputType.VERTEX_ID) {
                            final Object vertexId = edgeRecord.getOutV(edgeId.getUniqueId());
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
            return (FireflyId) element;
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
