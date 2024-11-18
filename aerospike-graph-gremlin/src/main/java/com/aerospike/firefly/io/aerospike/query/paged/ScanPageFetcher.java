package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.ScanHitCounter;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public class ScanPageFetcher<R extends Element> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanPageFetcher.class);
    private final String namespace;
    private final String set;
    private final ScanPolicy policy;
    private final BiFunction<Long, Long, Void> metricsCallback;
    private final long startTime;
    private final UUID scanId = UUID.randomUUID();
    private final ScanHitCounter scanHitCounter;


    public ScanPageFetcher(final FireflyGraph graph, final ScanPolicy policy, final String setName, final String namespace,
                           final int maxQueueSize, final int maxPageSize, final String mapKey, final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, maxQueueSize, transformKeyRecord);
        graph.getBaseGraph().configureScanPolicy(policy);
        this.policy = policy;
        this.policy.maxRecords = maxPageSize;
        this.namespace = namespace;
        this.set = setName;
        this.scanHitCounter = graph.getBaseGraph().getScanHitCounter();
        if (mapKey != null) {
            scanHitCounter.associateUUID(scanId, mapKey);
            scanHitCounter.increment(mapKey);
        }
        this.metricsCallback = (start, stop) -> {
            scanHitCounter.setScanTimings(scanId, start, stop);
            return null;
        };
        this.startTime = System.currentTimeMillis();
    }

    @Override
    protected void readPage() {
        final AtomicBoolean done = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final ScanPageFetcherRecordSequenceListener listener = new ScanPageFetcherRecordSequenceListener(done, latch);

        graph.getBaseGraph().scanPartitions(listener, policy, filter, set);
        metricsCallback.apply(startTime, System.currentTimeMillis());
        try {
            pageQueue.put(new Page(listener.paginationIterator));
            while (!done.get()) {
                // Monitor max wait to write to pagination queue.
                boolean succeeded = latch.await(graph.getBaseGraph().PAGINATION_PAGE_MAX_WAIT, TimeUnit.MILLISECONDS);
                if (!succeeded) {
                    listener.error("Failed to scan page: Timed out waiting for scan to complete.");
                    break;
                }
            }
        } catch (final InterruptedException e) {
            signalError("Error waiting for scan to complete: " + e.getMessage(), e);
        }
    }

    class ScanPageFetcherRecordSequenceListener implements RecordSequenceListener {
        final PaginationIterator<KeyRecord> paginationIterator;
        final AtomicBoolean done;
        final CountDownLatch latch;

        ScanPageFetcherRecordSequenceListener(final AtomicBoolean done, final CountDownLatch latch) {
            this.done = done;
            this.latch = latch;
            this.paginationIterator = new PaginationIterator<>(graph, () -> {
                if (!done.get()) {
                    closeCallback();
                }
            }, policy.totalTimeout == 0 ? policy.socketTimeout : policy.totalTimeout);
        }

        @Override
        public void onRecord(final Key key, final Record record) {
            paginationIterator.add(new KeyRecord(key, record));
        }

        @Override
        public void onSuccess() {
            done.set(true);
            latch.countDown();
            paginationIterator.close();
        }

        @Override
        public void onFailure(final AerospikeException exception) {
            synchronized (done) {
                if (done.get()) {
                    return;
                }
            }
            if (exception instanceof AerospikeException.ScanTerminated) {
                LOG.debug("Scan terminated.");
            } else {
                signalError("Failed to scan page: " + exception.getMessage(), exception);
            }
            done.set(true);
            latch.countDown();
            paginationIterator.close();
        }

        public void closeCallback() {
            synchronized (done) {
                shutdown();
                done.set(true);
                latch.countDown();
                paginationIterator.close();
                onFailure(new AerospikeException.ScanTerminated());
            }
        }

        public void error(final String message) {
            synchronized (done) {
                signalError(message);
                done.set(true);
                latch.countDown();
                paginationIterator.close();
                onFailure(new AerospikeException.ScanTerminated());
            }
        }
    }
}
