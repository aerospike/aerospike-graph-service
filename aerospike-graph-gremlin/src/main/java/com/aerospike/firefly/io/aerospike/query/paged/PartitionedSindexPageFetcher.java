package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionedSindexPageFetcher<R> extends SindexPageFetcher<R> {
    // Only used by computer.
    private final Object lock;
    private final List<AtomicBoolean> allCompleted;
    private final AtomicBoolean selfCompleted = new AtomicBoolean(false);
    private final int workerCount;
    private Boolean poisonPillInserted = false;

    public PartitionedSindexPageFetcher(final FireflyGraph graph,
                                        final QueryPolicy policy,
                                        final String setName, final String namespace,
                                        final Filter filter,
                                        final int maxPageSize,
                                        final FireflyGraph.TransformKeyRecord<R> transformKeyRecord,
                                        final String indexName,
                                        final PartitionFilter partitionFilter,
                                        final ExecutorService readLoopExecutorService,
                                        final BlockingQueue<Page> pageQueue,
                                        final Object lock,
                                        final List<AtomicBoolean> allCompleted,
                                        final int workerCount) {
        super(graph, policy, setName, namespace, filter, maxPageSize, transformKeyRecord, indexName, partitionFilter, readLoopExecutorService, pageQueue);
        this.lock = lock;
        this.allCompleted = allCompleted;
        this.workerCount = workerCount;
    }

    @Override
    protected void readPages() {
        readLoopExecutorService.submit(() -> {
            while (true) {
                try {
                    if (readLoopExecutorService.isShutdown() || isDone()) {
                        synchronized (lock) {
                            if (readLoopExecutorService.isShutdown()) {
                                try {
                                    if (!poisonPillInserted) {
                                        pageQueue.put(new PoisonPill());
                                        poisonPillInserted = true;
                                    }
                                } catch (final InterruptedException e) {
                                    signalError("Interrupted while attempting to add poison pill.", e);
                                }
                                return;
                            }
                            if (isDone()) {
                                // Set self done and check if all are done.
                                selfCompleted.set(true);
                                if (allCompleted.size() == workerCount && allCompleted.stream().allMatch(AtomicBoolean::get)) {
                                    // Shutdown if all done.
                                    readLoopExecutorService.shutdown();
                                }
                                try {
                                    if (!poisonPillInserted) {
                                        pageQueue.put(new PoisonPill());
                                        poisonPillInserted = true;
                                    }
                                } catch (final InterruptedException e) {
                                    signalError("Interrupted while attempting to add poison pill.", e);
                                }
                                return;
                            }
                        }
                    }
                    readPage();
                } catch (final Throwable e) {
                    if (e.getMessage() == null) {
                        signalError("Unexpected error while reading.", e);
                    } else {
                        signalError("Unexpected error while reading " + e.getMessage(), e);
                    }
                    return;
                }
            }
        });
    }
}
