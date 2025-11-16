package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeIdIterator implements CloseableIterator<FireflyId> {
    final protected AerospikeConnection db;
    final protected Iterator<KeyRecord> keyRecords;
    protected Iterator<Object> currentRecordIds = Collections.emptyIterator();

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
        while (true) {
            if (currentRecordIds.hasNext()) {
                return true;
            }

            if (!keyRecords.hasNext()) {
                return false;
            }

            getNextKeyRecords();
        }
    }

    @Override
    public FireflyId next() {
        if (hasNext()) {
            final ByteBuffer edgeId = (ByteBuffer) this.currentRecordIds.next();
            return this.db.getIdFactory().createEdgeId(edgeId);
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(this.keyRecords);
    }

    protected void getNextKeyRecords() {
        while (!this.currentRecordIds.hasNext()) {
            if (!this.keyRecords.hasNext()) {
                return;
            } else {
                this.currentRecordIds = ((Map<Object, ?>) this.keyRecords.next().record.getMap(db.getConfig().edgeDataBin))
                        .keySet().iterator();
            }
        }
    }
}
