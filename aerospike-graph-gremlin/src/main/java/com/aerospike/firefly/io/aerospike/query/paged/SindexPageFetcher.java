package com.aerospike.firefly.io.aerospike.query.paged;


import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private final QueryPolicy policy;
    private final Statement statement;

    public SindexPageFetcher(final FireflyGraph graph, final QueryPolicy policy, final String setName,
                             final String namespace, final Filter filter, final int maxQueueSize, final int maxPageSize,
                             final FireflyGraph.TransformKeyRecord<R> transformKeyRecord, final String indexName) {
        super(graph, maxQueueSize, transformKeyRecord, indexName);
        graph.getBaseGraph().configureReadPolicy(policy);
        this.policy = policy;
        this.statement = new Statement();
        this.statement.setNamespace(namespace);
        this.statement.setSetName(setName);
        this.statement.setFilter(filter);
        this.statement.setMaxRecords(maxPageSize);
    }

    @Override
    protected void readPage() {
        final RecordSet recordSet;
        try {
            recordSet = graph.getBaseGraph().getClient().queryPartitions(policy, statement, filter);
        } catch (final AerospikeException e) {
            signalError("Failed to read index: " + e.getMessage(), e);
            return;
        }

        final Iterator<KeyRecord> recordSetIterator = recordSet.iterator();
        PaginationIterator<KeyRecord> pi = new PaginationIterator<>(graph, recordSet::close);

        try {
            pageQueue.put(new Page(pi));
        } catch (final InterruptedException e) {
            signalError("Failed to add page to queue: " + e.getMessage(), e);
        }
        while (recordSetIterator.hasNext()) {
            pi.add(recordSetIterator.next());
        }
        pi.close();
    }
}
