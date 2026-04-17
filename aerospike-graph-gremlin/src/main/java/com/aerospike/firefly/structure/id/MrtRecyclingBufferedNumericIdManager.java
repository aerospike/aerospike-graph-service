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

import java.lang.ref.Cleaner;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.aerospike.firefly.structure.id.FireflyPhatEdgeId.getPhatEdgeStorageId;

public class MrtRecyclingBufferedNumericIdManager extends RecyclingEdgeIdManager<MrtEdgePackingIdManager> {
    private static final Logger LOG = LoggerFactory.getLogger(MrtRecyclingBufferedNumericIdManager.class);
    private final int packingSize;
    private final ThreadLocal<EdgePackIds> edgePackIds;
    // Must be LinkedHashMap due to FIFO ordering on the key set
    private final LinkedHashMap<Long, EdgePackIds> edgeRecordIdToPackIds = new LinkedHashMap<>();
    // Shared Set between Threads of current Edge record IDs that any thread owns. Required to prevent a thread from
    // grabbing an EdgePackIds created via recycling that may have the same Edge record ID that another thread is
    // generating from.
    private final Set<Long> inUseEdgeRecordIds = ConcurrentHashMap.newKeySet();

    protected MrtRecyclingBufferedNumericIdManager(final String uniqueIdCounterName,
                                                   final String packingIdCounterName, final long bufferSize,
                                                   final long recycleBufferSize, final int packingSize) {
        super(new MrtEdgePackingIdManager(packingIdCounterName, bufferSize),
                new IncrementingNumericIdManager(uniqueIdCounterName, recycleBufferSize), bufferSize);
        this.packingSize = packingSize;
        this.edgePackIds = ThreadLocal.withInitial(() -> null);
    }

    @Override
    public byte[] getNextId(final FireflyGraph graph) {
        final EdgePackIds ids = this.edgePackIds.get();
        if (ids == null || ids.isEmpty()) {
            // See if there is a recycled pack available if current pack is exhausted
            synchronized (this.edgeRecordIdToPackIds) {
                final Set<Long> recordIdSet = edgeRecordIdToPackIds.keySet();
                for (final Long recordId : recordIdSet) {
                    if (!inUseEdgeRecordIds.contains(recordId)) {
                        this.inUseEdgeRecordIds.add(recordId);
                        this.edgePackIds.set(this.edgeRecordIdToPackIds.remove(recordId));
                        return getNextId(graph);
                    }
                }
                // If none found getNextPackId will generate a new pack
            }
        }
        return getNextPackId(graph);
    }

    public void recycleCurrentPack() {
        final EdgePackIds ids = this.edgePackIds.get();
        if (ids != null) {
            this.edgePackIds.remove();
            ids.recycleCurrentPack();
        }
    }

    @Override
    protected byte[] getNewId(final FireflyGraph graph) {
        // Forcibly get a new pack to avoid MRT conflict
        final EdgePackIds ids = this.edgePackIds.get();
        ids.recycleCurrentPack();
        return getNextPackId(graph);
    }

    private byte[] getNextPackId(final FireflyGraph graph) {
        final EdgePackIds ids = this.edgePackIds.get();
        if (ids == null || ids.isEmpty()) {
            reserveNewEdgePackIds(graph);
            return this.getNextPackId(graph);
        }
        return ids.poll();
    }

    private synchronized void reserveNewEdgePackIds(final FireflyGraph graph) {
        Long packingId = this.packingIdManager.getNextId(graph);
        Long edgeRecordId = getPhatEdgeStorageId(packingId, this.packingSize);
        final EdgePackIds ids = new EdgePackIds(this, edgeRecordId);
        ids.add(longToBytes(packingId));
        // When this is 0 then packingId is the last ID before the next Edge pack so set the cutoff here
        while (this.packingIdManager.isIdBufferContinuous() && Math.floorMod(packingId, this.packingSize) != 0) {
            packingId = this.packingIdManager.getNextId(graph);
            edgeRecordId = getPhatEdgeStorageId(packingId, this.packingSize);
            if (!edgeRecordId.equals(ids.edgeRecordId)) {
                // This should never happen.
                final String error = "ID manager reserved IDs in multiple Edge record packs. Please contact support.";
                LOG.error(error);
                throw new IllegalStateException(error);
            } else {
                ids.add(longToBytes(packingId));
            }
        }
        this.inUseEdgeRecordIds.add(ids.edgeRecordId);
        this.edgePackIds.set(ids);
    }

    @Override
    public void recycleId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted) {
        final long recycledId;
        final long recordId;
        if (id instanceof FireflyPhatEdgeId) {
            recycledId = ((FireflyPhatEdgeId) id).getPackingId();
            recordId = (Long) id.getStorageId();
        } else {
            final String message = "Could not recycle ID of unexpected type " + id.getClass().getName();
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        if (this.edgeRecordIdToPackIds.size() >= bufferSize && !this.edgeRecordIdToPackIds.containsKey(recordId)) {
            LOG.debug("Recycled IDs buffer is full. Recycling ID {} will be dropped.", recycledId);
            return;
        }
        synchronized (this.edgeRecordIdToPackIds) {
            this.edgeRecordIdToPackIds.compute(recordId, (key, current) -> {
                final EdgePackIds edgePackIds = Objects.requireNonNullElseGet(current,
                        () -> new EdgePackIds(this, recordId));
                final byte[] recycledIdBytes = wasIdCommitted ? getRecycledId(graph, recycledId) : longToBytes(recycledId);
                edgePackIds.add(recycledIdBytes);
                return edgePackIds;
            });
        }
    }

    private byte[] getRecycledId(final FireflyGraph graph, final long recycledPackingId) {
        final byte[] id = new byte[16];
        final Long recycledUniqueId = this.uniqueIdManager.getNextId(graph);
        System.arraycopy(longToBytes(recycledPackingId), 0, id, 0, 8);
        System.arraycopy(longToBytes(recycledUniqueId), 0, id, 8, 8);
        return id;
    }

    /**
     * Simple wrapper class to ensure that if there are any threads that grab Edge IDs and then shut down that no IDs
     * are leaked when unused.
     *
     * Public for testing purposes.
     */
    static public class EdgePackIds {
        static private final Cleaner CLEANER = Cleaner.create();
        private final Queue<byte[]> ids;
        private final MrtRecyclingBufferedNumericIdManager idManager;
        private final Long edgeRecordId;
        private final Cleaner.Cleanable registeredRecycleAction;

        private EdgePackIds(final MrtRecyclingBufferedNumericIdManager idManager, final Long edgeRecordId) {
            this.idManager = idManager;
            this.edgeRecordId = edgeRecordId;
            this.ids = new ArrayDeque<>();
            this.registeredRecycleAction = CLEANER.register(this, new RecycleAction(this.idManager, this.edgeRecordId, this.ids));
        }

        static private class RecycleAction implements Runnable {
            private final MrtRecyclingBufferedNumericIdManager idManager;
            private final Long edgeRecordId;
            private final Queue<byte[]> ids;

            private RecycleAction(final MrtRecyclingBufferedNumericIdManager idManager, final Long edgeRecordId,
                                  final Queue<byte[]> ids) {
                this.idManager = idManager;
                this.edgeRecordId = edgeRecordId;
                this.ids = ids;
            }

            @Override
            public void run() {
                if (!this.ids.isEmpty()) {
                    if (this.idManager.edgeRecordIdToPackIds.size() >= this.idManager.bufferSize &&
                            !this.idManager.edgeRecordIdToPackIds.containsKey(this.edgeRecordId)) {
                        LOG.debug("Recycled IDs buffer is full. Dropping {} recycling IDs.", this.ids.size());
                    } else {
                        synchronized (this.idManager.edgeRecordIdToPackIds) {
                            this.idManager.edgeRecordIdToPackIds.compute(this.edgeRecordId, (key, current) -> {
                                final EdgePackIds edgePackIds = Objects.requireNonNullElseGet(current,
                                        () -> new EdgePackIds(this.idManager, this.edgeRecordId));
                                while (!this.ids.isEmpty()) {
                                    edgePackIds.add(this.ids.poll());
                                }
                                return edgePackIds;
                            });
                        }
                    }
                    this.ids.clear();
                    this.idManager.inUseEdgeRecordIds.remove(this.edgeRecordId);
                }
            }
        }

        private boolean isEmpty() {
            return this.ids.isEmpty();
        }

        private void add(final byte[] id) {
            this.ids.add(id);
        }

        private byte[] poll() {
            final byte[] id = this.ids.poll();
            if (this.ids.isEmpty()) {
                this.idManager.inUseEdgeRecordIds.remove(this.edgeRecordId);
            }
            return id;
        }

        private void recycleCurrentPack() {
            this.registeredRecycleAction.clean();
        }

        public int size() {
            return this.ids.size();
        }
    }

    /**
     * Testing function
     */
    public Map<Long, EdgePackIds> getRecycledPackIds() {
        return new HashMap<>(this.edgeRecordIdToPackIds);
    }

    /**
     * Testing function
     */
    public Set<Long> getInUseEdgeRecordIds() {
        return new HashSet<>(this.inUseEdgeRecordIds);
    }
}
