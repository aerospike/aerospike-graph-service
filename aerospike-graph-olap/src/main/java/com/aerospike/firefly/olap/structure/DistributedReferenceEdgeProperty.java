package com.aerospike.firefly.olap.structure;


import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;

import java.io.Serializable;

public class DistributedReferenceEdgeProperty<V> extends ReferenceProperty<V> implements Serializable {
    final ReferenceEdge edge;
    final String key;
    public DistributedReferenceEdgeProperty(final String key, final ReferenceEdge e) {
        super(key, null);
        this.key = key;
        this.edge = e;
    }

    @Override
    public Edge element() {
        return edge;
    }

    @Override
    public V value() {
        return (V) edge.id();
    }

    @Override
    public String toString() {
        return "DistributedReferenceEdgeProperty{" +
                "key=" + key +
                '}';
    }
}