package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.SizeEstimator;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BulkedRowSet implements Serializable {

    private final Map<Integer, Row> map = new ConcurrentHashMap<>();
    private final Queue<Row> collidedRows = new ConcurrentLinkedQueue<>();
    private final boolean bulkingSupported;
    transient private Codec codec;

    public BulkedRowSet(final Codec rowCodec) {
        this.codec = rowCodec;
        this.bulkingSupported = rowCodec.isBulkingSupported();
    }

    // Required for use after serialization.
    public void setCodec(final Codec rowCodec) {
        this.codec = rowCodec;
    }

    public Iterator<Row> iterator() {
        return IteratorUtils.concat(this.map.values().iterator(), this.collidedRows.iterator());
    }

    public long estimateSize() {
        long size = 0;
        for (final Row row : this.map.values()) {
            size += SizeEstimator.estimate(row);
        }
        for (final Row row : this.collidedRows) {
            size += SizeEstimator.estimate(row);
        }
        return size;
    }

    public <T> void add(final Traverser.Admin<T> traverser) {
        if (!bulkingSupported) {
            // Just throw in collided rows since we aren't bulking anyway.
            collidedRows.add(this.codec.encode(traverser));
            return;
        }

        // Traverser hashcode considers basically everything but the step id.
        final Integer hashCode = computeHashCode(traverser);
        final Row inserted = this.codec.encode(traverser);
        map.compute(hashCode, (key, existing) -> {
            if (existing == null) {
                return inserted;
            }
            
            final Object[] values = new Object[existing.size()];
            final int bulkOrdinal = codec.getBulkedOrdinal();
            for (int i = 0; i < existing.size(); i++) {
                if (i == bulkOrdinal) {
                    values[i] = (Long) existing.get(i) + traverser.bulk();
                } else {
                    // If the existing row is not the same as the inserted row, we have a hash collision.
                    // Use Objects.equals, because either of these can be null, and this handles nulls.
                    if (!Objects.equals(existing.get(i), inserted.get(i))) {
                        // Hash collision but not equal, insert the new row into the collided rows, and exit.
                        this.collidedRows.add(inserted);
                        return existing; // leave existing untouched
                    }
                    values[i] = existing.get(i);
                }
            }
            return RowFactory.create(values);
        });
    }

    public <T> void addAll(final Collection<Traverser.Admin<T>> traversers) {
        for (final Traverser.Admin<T> traverser : traversers) {
            this.add(traverser);
        }
    }

    // Traverser does not consider step with hash, so we need to add it to the hashcode.
    private <T> int computeHashCode(final Traverser.Admin<T> traverser) {
        return 31 * traverser.hashCode() + traverser.getStepId().hashCode();
    }

    // Func for metric logging if we ever want it.
    public int rowCount() {
        return map.size() + collidedRows.size();
    }

    // For testing only.
    public Queue<Row> getCollidedRows() {
        return collidedRows;
    }
}
