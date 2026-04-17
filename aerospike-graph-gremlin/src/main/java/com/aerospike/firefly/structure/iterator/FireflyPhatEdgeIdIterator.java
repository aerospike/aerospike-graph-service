/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
