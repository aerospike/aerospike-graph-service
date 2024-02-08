package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class SindexPageFetcher<R> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(SindexPageFetcher.class);
    final String namespace;
    final String set;
    final QueryPolicy policy;
    final Statement statement;
    boolean error = false;
    final Object lock = new Object();
    final AtomicBoolean done = new AtomicBoolean(false);

    public SindexPageFetcher(final FireflyGraph graph, final QueryPolicy policy, final String setName, final String namespace,
                             final Filter filter, final int maxQueueSize, final int maxPageSize, final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, maxQueueSize, transformKeyRecord);
        this.policy = policy;
        this.statement = new Statement();
        this.statement.setNamespace(namespace);
        this.statement.setSetName(setName);
        this.statement.setFilter(filter);
        this.statement.setMaxRecords(maxPageSize);
        policy.maxRecords = maxPageSize;
        this.namespace = namespace;
        this.set = setName;
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
                } catch (InterruptedException e) {
                    LOG.error("Error waiting for page to be ready.", e);
                }
            }
        }
        if (error) {
            throw new RuntimeException("Error fetching page.");
        }

    }

    class SindexPageFetcherRecordSequenceListener implements RecordSequenceListener {
        final List<KeyRecord> keyRecords = new LinkedList<>();

        SindexPageFetcherRecordSequenceListener() {
            error = false;
        }

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
            LOG.error("Error.", exception);

            // TODO something with this.
            error = true;
            synchronized (lock) {
                done.set(true);
                lock.notifyAll();
            }
        }
    }
}
