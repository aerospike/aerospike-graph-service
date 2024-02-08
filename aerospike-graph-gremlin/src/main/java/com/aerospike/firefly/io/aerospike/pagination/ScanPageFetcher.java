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
    protected void readPage() {
        final ScanPageFetcherScanCallback callback = new ScanPageFetcherScanCallback();

        // Need to lock otherwise we get duplicated data back since it submits multiple scans for same page.
        graph.getBaseGraph().getClient().scanPartitions(policy, filter, namespace, set, callback);

        try {
            pageQueue.put(new Page(callback.keyRecords));
        } catch (InterruptedException e) {
            LOG.error("Error adding poison pill.", e);
        }
    }

    class ScanPageFetcherScanCallback implements ScanCallback {

        final List<KeyRecord> keyRecords = new LinkedList<>();

        @Override
        public void scanCallback(final Key key, final Record record) throws AerospikeException {
            if (readLoopExecutorService.isShutdown()) {
                throw new AerospikeException.ScanTerminated();
            }
            keyRecords.add(new KeyRecord(key, record));
        }
    }
}
