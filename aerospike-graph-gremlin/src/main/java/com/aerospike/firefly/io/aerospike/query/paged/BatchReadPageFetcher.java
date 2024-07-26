package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.List;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;

public class BatchReadPageFetcher<R> extends PageFetcher<R> {
    private final BatchPolicy policy;
    private final List<Key> keysToRead;
    private final int maxPageSize;
    private int idx;

    public BatchReadPageFetcher(final FireflyGraph graph, final BatchPolicy policy, final Class<? extends FireflyElement> type,
                                final int maxQueueSize, final int maxPageSize, final Expression expression,
                                final FireflyGraph.TransformKeyRecord<R> transformKeyRecord, final List<Object> idsToRead) {
        super(graph, maxQueueSize, transformKeyRecord);
        graph.getBaseGraph().configureReadPolicy(policy);
        this.policy = policy;
        this.policy.sendKey = false;
        this.policy.filterExp = expression;
        if (idsToRead.size() == 1 && idsToRead.get(0) instanceof P) {
            // Passed in as P.within([id1, id2, ...])
            final P p = (P) idsToRead.get(0);
            if (!p.getBiPredicate().toString().equals("within")) {
                throw new IllegalArgumentException("Batch read only supports within predicate");
            }
            if (!(p.getValue() instanceof List)) {
                throw new IllegalArgumentException("Batch read only supports a single list of keys");
            }
            idsToRead.clear();
            idsToRead.addAll((List) p.getValue());
        }

        this.keysToRead = idsToRead.stream().
                map(id -> graph.getIdFactory().createId(id, type)).
                map(id -> getKey(graph.getBaseGraph(), graph.getBaseGraph().setFromElementType(type), id)).
                collect(Collectors.toList());
        this.idx = 0;
        this.maxPageSize = maxPageSize;
    }

    @Override
    protected void readPage() {
        final List<Key> keysToRead = this.keysToRead.subList(idx, Math.min(this.keysToRead.size(), idx + maxPageSize));
        final Record[] records = graph.getBaseGraph().getClient().get(policy, keysToRead.toArray(new Key[0]));
        final PaginationIterator<KeyRecord> pi = new PaginationIterator<>(graph, () -> {
        });
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
