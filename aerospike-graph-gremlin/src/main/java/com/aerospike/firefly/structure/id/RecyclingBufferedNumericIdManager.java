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

package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class RecyclingBufferedNumericIdManager extends RecyclingEdgeIdManager<DecrementingNumericIdManager> {
    private static final Logger LOG = LoggerFactory.getLogger(RecyclingBufferedNumericIdManager.class);
    protected final ConcurrentLinkedQueue<Long> recycledIds = new ConcurrentLinkedQueue<>();

    protected RecyclingBufferedNumericIdManager(final String packingIdCounterName,
                                                final String uniqueIdCounterName,
                                                final long bufferSize,
                                                final long recycleBufferSize) {
        super(new DecrementingNumericIdManager(packingIdCounterName, bufferSize),
                new IncrementingNumericIdManager(uniqueIdCounterName, recycleBufferSize), bufferSize);
    }

    @Override
    public synchronized byte[] getNextId(final FireflyGraph graph) {
        if (this.recycledIds.peek() == null) {
            return getNewId(graph);
        } else {
            return getRecycledId(graph);
        }
    }

    protected byte[] getNewId(final FireflyGraph graph) {
        final byte[] id = new byte[8];
        final Long newId = this.packingIdManager.getNextId(graph);
        System.arraycopy(longToBytes(newId), 0, id, 0, 8);
        return id;
    }

    protected byte[] getRecycledId(final FireflyGraph graph) {
        final byte[] id = new byte[16];
        final Long recycledPackingId = this.recycledIds.poll();
        final Long recycledUniqueId = this.uniqueIdManager.getNextId(graph);
        System.arraycopy(longToBytes(recycledPackingId), 0, id, 0, 8);
        System.arraycopy(longToBytes(recycledUniqueId), 0, id, 8, 8);
        return id;
    }

    @Override
    public void recycleId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted) {
        if (!wasIdCommitted) {
            // This should never happen
            throw new IllegalStateException("Attempted to recycle an uncommitted ID. Please contact support.");
        }
        final long recycledId;
        if (id instanceof FireflyPhatEdgeId) {
            recycledId = ((FireflyPhatEdgeId) id).getPackingId();
        } else {
            final String message = "Could not recycle ID of unexpected type " + id.getClass().getName();
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        if (this.recycledIds.size() >= bufferSize) {
            LOG.debug("Recycled IDs buffer is full. Recycling ID {} will be dropped.", recycledId);
            return;
        }
        this.recycledIds.add(recycledId);
    }

    public int availableRecycledIds() {
        return this.recycledIds.size();
    }
}
