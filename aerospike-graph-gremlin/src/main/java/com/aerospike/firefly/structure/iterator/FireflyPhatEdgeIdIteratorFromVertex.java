package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.IN_V_INDEX;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_INDEX;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_INDEX;

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
        final Map<ByteBuffer, List<?>> edgeIdToData = (Map<ByteBuffer, List<?>>) record.getMap(db.EDGE_DATA_BIN);

        if (this.direction == Direction.OUT || this.direction == Direction.BOTH) {
            final Set<ByteBuffer> outEdgeIds = getIndividualEdgeIdsAttachedToVertex(record, Direction.OUT);
            for (final ByteBuffer edgeId : outEdgeIds) {
                if (labels.isEmpty() || labels.contains((String) edgeIdToData.get(edgeId).get(LABEL_INDEX))) {
                    if (outputType == OutputType.VERTEX_ID) {
                        final byte[] vertexId = (byte[]) edgeIdToData.get(edgeId).get(IN_V_INDEX);
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
                if (labels.isEmpty() || labels.contains((String) edgeIdToData.get(edgeId).get(LABEL_INDEX))) {
                    if (outputType == OutputType.VERTEX_ID) {
                        final byte[] vertexId = (byte[]) edgeIdToData.get(edgeId).get(OUT_V_INDEX);
                        outputIds.add(vertexId);
                    } else {
                        outputIds.add(edgeId);
                    }
                }
            }
        }

        this.currentRecordIds = outputIds.iterator();
    }

    protected Set<ByteBuffer> getIndividualEdgeIdsAttachedToVertex(final Record record, final Direction direction) {
        final int directionIndex;
        if (direction == Direction.BOTH) {
            // Direction.BOTH should not be propagated here and should be combined at a higher level.
            throw new RuntimeException("Cannot get individual Edge IDs attached to a Vertex with Direction.BOTH");
        } else {
            directionIndex = direction == Direction.OUT ? OUT_V_INDEX : IN_V_INDEX;
        }

        final Set<ByteBuffer> attachedEdgeIds = new HashSet<>();
        final Map<ByteBuffer, List<?>> edgeDataMap = (Map<ByteBuffer, List<?>>) record.getMap(db.EDGE_DATA_BIN);
        for (final Map.Entry<ByteBuffer, List<?>> edgeDataEntry : edgeDataMap.entrySet()) {
            final List<?> edgeData = edgeDataEntry.getValue();
            final byte[] vertexIdBytes = (byte[]) edgeData.get(directionIndex);
            if (Arrays.equals(vertexIdBytes, this.vertexId.getKeyHash())) {
                attachedEdgeIds.add(edgeDataEntry.getKey());
            }
        }
        return attachedEdgeIds;
    }

    @Override
    public FireflyId next() {
        if (hasNext()) {
            final Object element = this.currentRecordIds.next();
            if (element == null) {
                throw FastNoSuchElementException.instance();
            }
            if (outputType.equals(OutputType.VERTEX_ID)) {
                return FireflyIdPoly.fromHash((byte[]) element, db.VERTEX_AERO_SET);
            } else {
                return new FireflyPhatEdgeId((ByteBuffer) element, this.db.PHAT_EDGE_SIZE, this.db.EDGE_AERO_SET);
            }
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
