package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.aerospike.firefly.structure.FireflyVertex.SUPERNODE_PROPERTY_KEY;

/**
 * Class to represent the virtual property ~supernode denoting a supernode vertex.
 *
 * This is functionally identical to VertexProperty.empty() but can handle being predicate checked against without
 * throwing an exception within Tinkerpop.
 */
public class FireflyVirtualSupernodeVertexProperty<V> implements VertexProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVirtualSupernodeVertexProperty.class);
    private final String key = SUPERNODE_PROPERTY_KEY;
    private final Vertex vertex;

    public FireflyVirtualSupernodeVertexProperty(final FireflyVertex vertex) {
        this.vertex = vertex;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        throw new NoSuchElementException("Virtual property ~supernode has no value.");
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        final String message = "Virtual property ~supernode cannot be removed.";
        LOG.error(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    public Object id() {
        final String message = "Virtual property ~supernode does not have an ~id.";
        LOG.error(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        final String message = "Virtual property ~supernode does not support properties.";
        LOG.error(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        final String message = "Virtual property ~supernode does not support properties.";
        LOG.error(message);
        throw new UnsupportedOperationException(message);
    }
}
