package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(SindexPageFetcher.class);
    private final QueryPolicy policy;
    private final Statement statement;
    private String error = "";
    private final Object lock = new Object();
    private final AtomicBoolean done = new AtomicBoolean(false);

    public SindexPageFetcher(final FireflyGraph graph, final QueryPolicy policy, final String setName, final String namespace,
                             final Filter filter, final int maxQueueSize, final int maxPageSize, final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, maxQueueSize, transformKeyRecord);
        this.policy = policy;
        this.statement = new Statement();
        this.statement.setNamespace(namespace);
        this.statement.setSetName(setName);
        this.statement.setFilter(filter);
        this.statement.setMaxRecords(maxPageSize);
    }

    @Override
    protected void readPage() {
        done.set(false);
        graph.getBaseGraph().getClient().queryPartitions(graph.getBaseGraph().eventLoops.next(),
                new SindexPageFetcherRecordSequenceListener(), policy, statement, filter);
        synchronized (lock) {
            while (!done.get()) {
                try {
                    lock.wait();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    try {
                        pageQueue.put(new ErrorPage("Error waiting for page to be ready. " + e.getMessage()));
                    } catch (final InterruptedException e2) {
                        LOG.error("Error adding signalling error to iterator.", e2);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        if (!error.isEmpty()) {
            try {
                pageQueue.put(new ErrorPage(error));
            } catch (final InterruptedException e) {
                LOG.error("Error adding signalling error to iterator.", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    class SindexPageFetcherRecordSequenceListener implements RecordSequenceListener {
        final List<KeyRecord> keyRecords = new LinkedList<>();

        @Override
        public void onRecord(final Key key, final Record record) throws AerospikeException {
            if (readLoopExecutorService.isShutdown()) {
                throw new AerospikeException.QueryTerminated();
            }
            keyRecords.add(new KeyRecord(key, record));
        }

        @Override
        public void onSuccess() {
            pageQueue.add(new Page(keyRecords));
            synchronized (lock) {
                done.set(true);
                lock.notifyAll();
            }
        }

        @Override
        public void onFailure(AerospikeException exception) {
            LOG.error("Sindex fetch failure.", exception);
            error = exception.getMessage();
            synchronized (lock) {
                done.set(true);
                lock.notifyAll();
            }
        }
    }
}
