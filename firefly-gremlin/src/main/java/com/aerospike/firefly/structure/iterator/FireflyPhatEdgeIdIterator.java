package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeIdIterator implements CloseableIterator<FireflyId> {
    final protected AerospikeConnection db;
    final protected Iterator<KeyRecord> keyRecords;
    protected Iterator<Long> currentRecordEdgeIds = Collections.emptyIterator();
    /**
     * Wrapper iterator for converting key records of Phat Edges into all of its contained edges' FireflyIds.
     *
     * @param keyRecordIterator KeyRecord iterator to wrap.
     * @param db                AerospikeConnection instance.
     */
    public FireflyPhatEdgeIdIterator(final Iterator<KeyRecord> keyRecordIterator,
                                     final AerospikeConnection db) {
        this.db = db;
        this.keyRecords = keyRecordIterator;
    }
    @Override
    public boolean hasNext() {
        if (!currentRecordEdgeIds.hasNext()) {
            if (!keyRecords.hasNext()) {
                return false;
            } else {
                getNextKeyRecords();
                return hasNext();
            }
        } else {
            return true;
        }
    }
    @Override
    public FireflyId next() {
        if (hasNext()) {
            final long edgeId = this.currentRecordEdgeIds.next();
            return new FireflyPhatEdgeId(edgeId, this.db.PHAT_EDGE_SIZE, this.db.EDGE_AERO_SET);
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
    protected void getNextKeyRecords() {
        this.currentRecordEdgeIds = ((Map<Long, String>) this.keyRecords.next().record.getMap(AerospikeConnection.LABEL))
                .keySet().iterator();
    }
}
