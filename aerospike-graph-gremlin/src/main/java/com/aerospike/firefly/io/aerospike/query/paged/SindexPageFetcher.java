package com.aerospike.firefly.io.aerospike.query.paged;


import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;

import java.util.Iterator;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private final QueryPolicy policy;
    private final Statement statement;

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
    }

    @Override
    protected void readPage() {
        final RecordSet recordSet;
        try {
            recordSet = graph.getBaseGraph().queryPartitions(policy, statement, filter);
        } catch (final AerospikeGraphException e) {
            signalError("Failed to read index: " + e.getMessage(), e);
            return;
        }

        final Iterator<KeyRecord> recordSetIterator = recordSet.iterator();
        try (final PaginationIterator<KeyRecord> pi = new PaginationIterator<>(graph, recordSet::close, this.policy.totalTimeout)) {
            pageQueue.put(new Page(pi));
            while (recordSetIterator.hasNext()) {
                pi.add(recordSetIterator.next());
            }
        } catch (final InterruptedException e) {
            throw new TraversalInterruptedException();
        } catch (final Exception e) {
            if (e instanceof TraversalInterruptedException) {
                throw (TraversalInterruptedException) e;
            }
            signalError("Encountered exception while attempting to read index: " + e.getMessage(), e);
        }
    }
}
