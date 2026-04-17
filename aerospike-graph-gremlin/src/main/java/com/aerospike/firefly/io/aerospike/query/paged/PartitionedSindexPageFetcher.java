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

package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionedSindexPageFetcher<R> extends SindexPageFetcher<R> {
    // Only used by computer.
    private final Object lock;
    private final List<AtomicBoolean> allCompleted;
    private final int workerCount;
    private Boolean poisonPillInserted = false;

    public PartitionedSindexPageFetcher(final FireflyGraph graph,
                                        final QueryPolicy policy,
                                        final String setName,
                                        final String namespace,
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

    // Olap single threaded partition reader.
    public PartitionedSindexPageFetcher(final FireflyGraph graph,
                                        final QueryPolicy policy,
                                        final String setName,
                                        final String namespace,
                                        final Filter filter,
                                        final int maxPageSize,
                                        final FireflyGraph.TransformKeyRecord<R> transformKeyRecord,
                                        final String indexName,
                                        final PartitionFilter partitionFilter,
                                        final BlockingQueue<Page> pageQueue) {
        super(graph, policy, setName, namespace, filter, maxPageSize, transformKeyRecord, indexName, partitionFilter, Executors.newSingleThreadExecutor(), pageQueue);
        this.lock = new Object();
        this.allCompleted = List.of(new AtomicBoolean(false));
        this.workerCount = 1;
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
