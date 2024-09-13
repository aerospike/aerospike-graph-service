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
        this.statement.setMaxRecords(maxPageSize * 10);
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
        List<PaginationIterator<KeyRecord>> pis = new ArrayList<>();
        final CloseRecordSet closeRecordSet = new CloseRecordSet(recordSet);
        for (int i = 0; i < 10; i++) {
            pis.add(new PaginationIterator<>(graph, closeRecordSet));
        }

        try {
            // Convert into multiple pages
            for (int i = 0; i < 10; i++) {
                pageQueue.put(new Page(pis.get(i)));
            }
        } catch (final InterruptedException e) {
            signalError("Failed to add page to queue: " + e.getMessage(), e);
        }
        int idx = 0;
        int count = 0;
        while (recordSetIterator.hasNext()) {
            if (count > this.statement.getMaxRecords() / 10) {
                count = 0;
                idx++;
            }
            pis.get(idx).add(recordSetIterator.next());
        }
        for (int i = 0; i < 10; i++) {
            pis.get(i).close();
        }
    }

    class CloseRecordSet implements Runnable {
        // Close if all iterators are closed
        int count = 0;
        final RecordSet recordSet;

        CloseRecordSet(RecordSet recordSet) {
            this.recordSet = recordSet;
        }

        @Override
        public void run() {
            count++;
            if (count == 10) {
                recordSet.close();
            }
        }
    }
}
