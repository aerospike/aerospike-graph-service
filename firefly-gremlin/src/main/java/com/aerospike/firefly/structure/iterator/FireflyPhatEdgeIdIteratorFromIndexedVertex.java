package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;

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
     */
    public FireflyPhatEdgeIdIteratorFromIndexedVertex(final Iterator<KeyRecord> keyRecordIterator,
                                                      final AerospikeConnection db, final Direction direction,
                                                      final FireflyId vertexId) {
        super(keyRecordIterator, db, direction, vertexId);
    }

    @Override
    protected String getInVBinName() {
        return this.db.SUPERNODES_IN;
    }

    @Override
    protected String getOutVBinName() {
        return this.db.SUPERNODES_OUT;
    }
}
