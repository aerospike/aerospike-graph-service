package com.aerospike.firefly.io.aerospike.query.paged;


import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(SindexPageFetcher.class);
    private final QueryPolicy policy;
    private final Statement statement;

    public SindexPageFetcher(final FireflyGraph graph, final QueryPolicy policy, final String setName, final String namespace,
                             final Filter filter, final int maxQueueSize, final int maxPageSize, final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, maxQueueSize, transformKeyRecord);
        this.policy = policy;
        this.policy.socketTimeout = graph.getBaseGraph().AEROSPIKE_SOCKET_TIMEOUT;
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
            signalError("Failed to read index: " + e.getMessage());
            return;
        }

        final Iterator<KeyRecord> recordSetIterator = recordSet.iterator();
        final PaginationIterator<KeyRecord> pi = new PaginationIterator<>(graph, recordSet::close);

        try {
            pageQueue.put(new Page(pi));
        } catch (final InterruptedException e) {
            signalError("Failed to add page to queue: " + e.getMessage());
        }
        while (recordSetIterator.hasNext()) {
            pi.add(recordSetIterator.next());
        }
        pi.close();
    }
}
