package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Iterator;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private final QueryPolicy policy;
    private final Statement statement;
    private final int timeout;

    public SindexPageFetcher(final FireflyGraph graph, final QueryPolicy policy, final String setName,
                             final String namespace, final Filter filter, final int maxQueueSize, final int maxPageSize,
                             final FireflyGraph.TransformKeyRecord<R> transformKeyRecord, final String indexName) {
        super(graph, maxQueueSize, transformKeyRecord, indexName);
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
            pageQueue.put(new Page(pi));
            while (recordSetIterator.hasNext()) {
                pi.add(recordSetIterator.next());
            }
        }
    }
}
