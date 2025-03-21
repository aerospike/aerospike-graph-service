package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class BulkedRowSet implements Serializable {

    private final Map<Integer, Row> map = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Codec codec;
    private final StructType schema;
    private final boolean bulkingSupported;

    public BulkedRowSet(final Codec rowCodec) {
        this.codec = rowCodec;
        this.schema = rowCodec.getSchema();
        this.bulkingSupported = rowCodec.isBulkingSupported();
    }

    public Iterator<Row> iterator() {
        return this.map.values().iterator();
    }

    public synchronized <T> void add(final Traverser.Admin<T> traverser) {
        if (!bulkingSupported) {
            this.map.put(traverser.hashCode(), this.codec.encode(traverser));
            return;
        }

        // Just store hashcode to avoid storing the whole traverser.
        final Integer hashCode = traverser.hashCode();
        if (this.map.containsKey(traverser.hashCode())) {
            final Row existing = this.map.get(traverser.hashCode());
            final Object[] values = new Object[existing.size()];
            final int ordinal = codec.getBulkedOrdinal();
            for (int i = 0; i < existing.size(); i++) {
                if (i == ordinal) {
                    values[i] = (Long) existing.get(i) + traverser.bulk();
                } else {
                    values[i] = existing.get(i);
                }
            }
            this.map.put(hashCode, RowFactory.create(values, schema));
        } else {
            this.map.put(hashCode, this.codec.encode(traverser));
        }
    }

    public <T> void addAll(final Collection<Traverser.Admin<T>> traversers) {
        for (final Traverser.Admin<T> traverser : traversers) {
            this.add(traverser);
        }
    }

    public <T> Row get(final Traverser.Admin<T> traverser) {
        return this.map.get(traverser.hashCode());
    }

    public int size() {
        return this.map.size();
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }
}
