package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

public class ScanPageFetcher<R extends Element> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanPageFetcher.class);
    private final String namespace;
    private final String set;
    private final ScanPolicy policy;
    private final BiFunction<Long, Long, Void> metricsCallback;
    private final long startTime;

    public ScanPageFetcher(final FireflyGraph graph, final ScanPolicy policy, final String setName, final String namespace,
                           final int maxQueueSize, final int maxPageSize, final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, maxQueueSize, transformKeyRecord);
        this.policy = policy;
        policy.maxRecords = maxPageSize;
        this.namespace = namespace;
        this.set = setName;
        this.metricsCallback = (start, stop) -> {
            graph.getBaseGraph().getScanHitCounter().setScanTimings(UUID.randomUUID(), start, stop);
            return null;
        };
        this.startTime = System.currentTimeMillis();
    }

    @Override
    protected void readPage() {
        final ScanPageFetcherScanCallback callback = new ScanPageFetcherScanCallback();

        // Need to lock otherwise we get duplicated data back since it submits multiple scans for same page.
        graph.getBaseGraph().getClient().scanPartitions(policy, filter, namespace, set, callback);
        metricsCallback.apply(startTime, System.currentTimeMillis());

        try {
            System.out.println("Placing page " + callback.keyRecords.size() + " into queue.");
            pageQueue.put(new Page(callback.keyRecords));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            try {
                pageQueue.put(new ErrorPage("Error adding page to queue. " + e.getMessage()));
            } catch (final InterruptedException e2) {
                LOG.error("Error adding signalling error to iterator.", e2);
                Thread.currentThread().interrupt();
            }
        }
    }

    class ScanPageFetcherScanCallback implements ScanCallback {

        final List<KeyRecord> keyRecords = new LinkedList<>();

        @Override
        public void scanCallback(final Key key, final Record record) throws AerospikeException {
            if (readLoopExecutorService.isShutdown()) {
                throw new AerospikeException.ScanTerminated();
            }
            System.out.println("Adding keyrecord " + key + " to list.");
            keyRecords.add(new KeyRecord(key, record));
        }
    }
}
