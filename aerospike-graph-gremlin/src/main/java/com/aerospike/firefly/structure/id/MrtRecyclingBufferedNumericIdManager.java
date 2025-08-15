package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;

public class MrtRecyclingBufferedNumericIdManager extends RecyclingBufferedNumericIdManager {
    private static final Logger LOG = LoggerFactory.getLogger(MrtRecyclingBufferedNumericIdManager.class);
    private final int packingSize;
    private final ThreadLocal<EdgePackIds> edgePackIds;

    protected MrtRecyclingBufferedNumericIdManager(final String uniqueIdCounterName,
                                                   final String packingIdCounterName, final long bufferSize,
                                                   final long recycleBufferSize, final int packingSize) {
        super(uniqueIdCounterName, packingIdCounterName, bufferSize, recycleBufferSize);
        this.packingSize = packingSize;
        this.edgePackIds = new ThreadLocal<>();
    }

    @Override
    public synchronized byte[] getNextId(final FireflyGraph graph) {
        // MRT Edge Id Manager prefers new IDs until a pack is exhausted
        final EdgePackIds ids = this.edgePackIds.get();
        if ((ids == null || ids.isEmpty()) && this.recycledIds.peek() != null) {
            return getRecycledId(graph);
        } else {
            return getNewId(graph);
        }
    }

    @Override
    protected byte[] getNewId(final FireflyGraph graph) {
        final EdgePackIds ids = this.edgePackIds.get();
        if (ids == null || ids.isEmpty()) {
            reserveEdgePackIds(graph);
            return this.getNewId(graph);
        }
        final byte[] id = new byte[8];
        final long newId = ids.poll();
        System.arraycopy(longToBytes(newId), 0, id, 0, 8);
        return id;
    }

    private synchronized void reserveEdgePackIds(final FireflyGraph graph) {
        final EdgePackIds ids = new EdgePackIds(this);
        while (true) {
            final Long id = this.packingIdManager.getNextId(graph);
            ids.add(id);
            if (Math.floorMod(id, packingSize) == 0) {
                // This is the last ID before the next Edge pack so set the cutoff here.
                break;
            }
        }
        this.edgePackIds.set(ids);
    }

    /**
     * Simple wrapper class to ensure that if there are any threads that grab Edge IDs and then shut down that no IDs
     * are leaked when unused.
     */
    static private class EdgePackIds {
        private final Queue<Long> ids = new ArrayDeque<>();
        private final MrtRecyclingBufferedNumericIdManager idManager;

        private EdgePackIds(final MrtRecyclingBufferedNumericIdManager idManager) {
            this.idManager = idManager;
        }

        private boolean isEmpty() {
            return ids.isEmpty();
        }

        private void add(final Long id) {
            this.ids.add(id);
        }

        private Long poll() {
            return this.ids.poll();
        }

        private int size() {
            return this.ids.size();
        }

        @Override
        protected void finalize() {
            while (!this.ids.isEmpty()) {
                if (this.idManager.recycledIds.size() >= idManager.bufferSize) {
                    LOG.warn("Recycled IDs buffer is full. Dropping {} recycling IDs.", this.ids.size());
                    break;
                } else {
                    this.idManager.recycledIds.add(this.ids.poll());
                }
            }
        }
    }
}
