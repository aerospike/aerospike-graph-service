package com.aerospike.firefly.olap.structure;


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.io.Serializable;

public class DistributedReferenceVertexProperty<V> extends ReferenceVertexProperty<V> implements Serializable {
    final ReferenceVertex vertex;
    public DistributedReferenceVertexProperty(final Object id, final Object vertexId) {
        super(id, null, null);
        this.vertex = new ReferenceVertex(vertexId);
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public String toString() {
        return "DistributedReferenceVertexProperty{" +
                "vertex=" + vertex +
                '}';
    }
}