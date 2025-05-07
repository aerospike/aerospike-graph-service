package com.aerospike.firefly.olap.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.util.Map;

public class MutableDetachedVertexProperty extends DetachedVertexProperty {
    protected MutableDetachedVertexProperty(final VertexProperty vertexProperty, final boolean withProperties) {
        super(vertexProperty, withProperties);
    }

    public MutableDetachedVertexProperty(final Object id, final String label, final Object value, final Map properties, final Vertex vertex) {
        super(id, label, value, properties, vertex);
    }

    public MutableDetachedVertexProperty(final Object id, final String label, final Object value, final Map properties) {
        super(id, label, value, properties);
    }

    public void setValue(final Object value ) {
        this.value = value;
    }
}
