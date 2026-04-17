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
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private final QueryPolicy policy;
    private final Statement statement;
    private final int timeout;

    public SindexPageFetcher(final FireflyGraph graph,
                             final QueryPolicy policy,
                             final String setName,
                             final String namespace,
                             final Filter filter,
                             final int maxPageSize,
                             final FireflyGraph.TransformKeyRecord<R> transformKeyRecord,
                             final String indexName) {
        super(graph, transformKeyRecord, indexName);
        graph.getBaseGraph().configureQueryPolicy(policy);
        this.policy = policy;
        this.statement = new Statement();
        this.statement.setNamespace(namespace);
        this.statement.setSetName(setName);
        this.statement.setFilter(filter);
        this.statement.setMaxRecords(maxPageSize);
        this.timeout = policy.totalTimeout == 0 ? policy.socketTimeout : policy.totalTimeout;
    }

    public SindexPageFetcher(final FireflyGraph graph,
                             final QueryPolicy policy,
                             final String setName,
                             final String namespace,
                             final Filter filter,
                             final int maxPageSize,
                             final FireflyGraph.TransformKeyRecord<R> transformKeyRecord,
                             final String indexName,
                             final PartitionFilter partitionFilter,
                             final ExecutorService readLoopExecutorService,
                             final BlockingQueue<Page> pageQueue) {
        super(graph, transformKeyRecord, indexName, partitionFilter, readLoopExecutorService, pageQueue);
        this.policy = policy;
        this.statement = new Statement();
        this.statement.setNamespace(namespace);
        this.statement.setSetName(setName);
        this.statement.setFilter(filter);
        this.statement.setMaxRecords(maxPageSize);
        this.timeout = policy.totalTimeout == 0 ? policy.socketTimeout : policy.totalTimeout;
    }

    @Override
    protected void readPage() throws InterruptedException {
        final AerospikeConnection.FireflyRecordSet recordSet = graph.getBaseGraph().queryPartitions(policy, statement, filter);
        final Iterator<KeyRecord> recordSetIterator = recordSet.iterator();
        try (final PaginationIterator<KeyRecord> pi = new PaginationIterator<>(graph, recordSet::close, timeout)) {
            if (recordSetIterator.hasNext()) {
                pageQueue.put(new Page(pi));
            }
            while (recordSetIterator.hasNext()) {
                pi.add(recordSetIterator.next());
            }
        } catch (Exception e) {
            throw e;
        }
    }
}
