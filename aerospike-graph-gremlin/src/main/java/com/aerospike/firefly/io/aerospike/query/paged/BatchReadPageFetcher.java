package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.List;

public class BatchReadPageFetcher<R> extends PageFetcher<R> {
    private final Expression filterExp;
    private final List<Key> keysToRead;
    private final int maxPageSize;
    private int idx;
    private Long evaluationTimeout;

    public BatchReadPageFetcher(final FireflyGraph graph,
                                final int maxPageSize,
                                final Expression expression,
                                final FireflyGraph.TransformKeyRecord<R> transformKeyRecord,
                                final List<Key> keysToRead,
                                final Long evaluationTimeout) {
        super(graph, transformKeyRecord, null);
        this.filterExp = expression;
        this.keysToRead = keysToRead;
        this.idx = 0;
        this.maxPageSize = maxPageSize;
        this.evaluationTimeout = evaluationTimeout == 0 ? Long.MAX_VALUE : evaluationTimeout;
    }

    @Override
    protected void readPage() {
        final List<Key> keysToRead = this.keysToRead.subList(idx, Math.min(this.keysToRead.size(), idx + maxPageSize));
        final Record[] records = graph.getBaseGraph().dynamicBatchRead(
                keysToRead.toArray(new Key[0]), filterExp, graph.getBaseGraph().cacheManager.getTransactionCache());
        final PaginationIterator<KeyRecord> pi = new PaginationIterator<>(graph, () -> {}, evaluationTimeout);
        try {
            pageQueue.put(new Page(pi));
        } catch (final InterruptedException e) {
            signalError("Failed to add page to queue: " + e.getMessage(), e);
        }
        for (int i = 0; i < records.length; i++) {
            if (records[i] != null)
                pi.add(new KeyRecord(keysToRead.get(i), records[i]));
        }
        pi.close();
        idx += keysToRead.size();
    }

    @Override
    protected boolean isDone() {
        return idx >= keysToRead.size();
    }
}
