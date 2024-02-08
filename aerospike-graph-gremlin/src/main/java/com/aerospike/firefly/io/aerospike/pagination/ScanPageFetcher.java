package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ScanPageFetcher<R extends Element> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanPageFetcher.class);
    final String namespace;
    final String set;
    final ScanPolicy policy;

    public ScanPageFetcher(final FireflyGraph graph,
                           final ScanPolicy policy,
                           final String setName,
                           final String namespace,
                           final int readThreadCount,
                           final int maxQueueSize,
                           final int maxPageSize,
                           final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, readThreadCount, maxQueueSize, transformKeyRecord);
        this.policy = policy;
        policy.maxRecords = maxPageSize;
        this.namespace = namespace;
        this.set = setName;
    }

    @Override
    protected void readPage(final AerospikeClient client, final Object PAGE_LOCK) {
        final CountDownLatch countdownLatch = new CountDownLatch(1);
        final PageFetcherScanRecordSequenceListener listener = new PageFetcherScanRecordSequenceListener(countdownLatch);

        // Need to lock otherwise we get duplicated data back since it submits multiple scans for same page.
        synchronized (PAGE_LOCK) {
            client.scanPartitions(graph.getBaseGraph().eventLoops.next(), listener, policy, filter, namespace, set);
        }

        try {
            countdownLatch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for page to be read", e);
        }
        if (listener.hadError) {
            LOG.error("Error reading page");
        } else {
            pageQueue.add(new Page(listener.keyRecords));
        }
    }

    class PageFetcherScanRecordSequenceListener implements RecordSequenceListener {

        final List<KeyRecord> keyRecords = new LinkedList<>();
        boolean hadError = false;
        final CountDownLatch countdownLatch;

        public PageFetcherScanRecordSequenceListener(final CountDownLatch countdownLatch) {
            this.countdownLatch = countdownLatch;
        }

        @Override
        public void onRecord(final Key key, final Record record) throws AerospikeException {
            if (pageReaderExecutorService.isShutdown()) {
                throw new AerospikeException.ScanTerminated();
            }
            keyRecords.add(new KeyRecord(key, record));
        }

        @Override
        public void onSuccess() {
            countdownLatch.countDown();
        }

        @Override
        public void onFailure(final AerospikeException ae) {
            LOG.error("Error reading page", ae);
            hadError = true;
            countdownLatch.countDown();
        }
    }
}
