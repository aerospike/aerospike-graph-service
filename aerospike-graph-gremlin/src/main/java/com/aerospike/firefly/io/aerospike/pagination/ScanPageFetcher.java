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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;

public class ScanPageFetcher<R extends Element> extends PageFetcher<R> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanPageFetcher.class);
    private final String namespace;
    private final String set;
    private final ScanPolicy policy;
    private final BiFunction<Long, Long, Void> metricsCallback;
    private final long startTime;
    private final UUID scanId = UUID.randomUUID();


    public ScanPageFetcher(final FireflyGraph graph, final ScanPolicy policy, final String setName, final String namespace,
                           final int maxQueueSize, final int maxPageSize, final String mapKey, final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        super(graph, maxQueueSize, transformKeyRecord);
        this.policy = policy;
        policy.maxRecords = maxPageSize;
        this.namespace = namespace;
        this.set = setName;
        if (mapKey != null) {
            this.graph.getBaseGraph().scanHitCounterThreadLocal.get().associateUUID(scanId, mapKey);
        }
        this.metricsCallback = (start, stop) -> {
            graph.getBaseGraph().getScanHitCounter().setScanTimings(scanId, start, stop);
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
            final List<KeyRecord> keyRecords = new ArrayList<>(callback.keyRecords);
            pageQueue.put(new Page(keyRecords));
        } catch (final InterruptedException e) {
            signalError("Failed to add page to queue: " + e.getMessage());
        }
    }

    class ScanPageFetcherScanCallback implements ScanCallback {
        final Queue<KeyRecord> keyRecords = new ConcurrentLinkedQueue<>();

        @Override
        public void scanCallback(final Key key, final Record record) throws AerospikeException {
            if (readLoopExecutorService.isShutdown()) {
                throw new AerospikeException.ScanTerminated();
            }
            keyRecords.add(new KeyRecord(key, record));
        }
    }
}
