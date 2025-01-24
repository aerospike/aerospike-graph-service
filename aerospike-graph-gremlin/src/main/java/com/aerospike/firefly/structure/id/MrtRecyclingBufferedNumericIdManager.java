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

    protected MrtRecyclingBufferedNumericIdManager(final String recyclingIdCounterName,
                                                   final String uniqueIdCounterName, final long bufferSize,
                                                   final int packingSize) {
        super(recyclingIdCounterName, uniqueIdCounterName, bufferSize);
        this.packingSize = packingSize;
        this.edgePackIds = new ThreadLocal<>();
    }

    @Override
    protected long getRecycledId(final FireflyGraph graph) {
        final EdgePackIds ids = this.edgePackIds.get();
        if (ids == null || ids.isEmpty()) {
            reserveEdgePackIds(graph);
            return getRecycledId(graph);
        }
        return ids.poll();
    }

    private synchronized void reserveEdgePackIds(final FireflyGraph graph) {
        final EdgePackIds ids = new EdgePackIds(this);
        if (this.recycledIds.size() >= packingSize) {
            while (ids.size() < packingSize) {
                final Long id = this.recycledIds.poll();
                if (id == null) {
                    break;
                } else {
                    ids.add(id);
                }
            }
        } else {
            while (true) {
                final Long id = this.recyclingIdManager.getNextId(graph);
                ids.add(id);
                if (Math.abs(id) % packingSize == packingSize - 1) {
                    // This is the last ID before the next Edge pack so set the cutoff here.
                    break;
                }
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
            for (final Long id : ids) {
                if (this.idManager.recycledIds.size() >= idManager.bufferSize) {
                    LOG.warn("Recycled IDs buffer is full. Recycling ID {} will be dropped.", id);
                } else {
                    this.idManager.recycledIds.add(id);
                }
            }
        }
    }
}
