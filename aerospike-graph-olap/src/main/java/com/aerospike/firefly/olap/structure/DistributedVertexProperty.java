package com.aerospike.firefly.olap.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedVertexProperty<V> extends DistributedElement implements VertexProperty {
    private final String key;
    private final V value;

    public DistributedVertexProperty(final String key, final V value) {
        super(null, 0, key);
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public boolean isPresent() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Vertex element() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterator<Property> properties(final String... propertyKeys) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String toString() {
        return null;
    }
}
