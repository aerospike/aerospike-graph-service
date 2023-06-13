package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class FireflyStorageDebuggingVertexProperty implements VertexProperty<Map<String,Object>> {
    private final Map<String, Object> debug;
    private final FireflyVertex vertex;

    public FireflyStorageDebuggingVertexProperty(FireflyVertex fireflyVertex) {
        this.vertex = fireflyVertex;
        this.debug = fireflyVertex.debugStorage();
    }

    @Override
    public String key() {
        return FireflyElement.DEBUG_STORAGE_PROPERTY;
    }

    @Override
    public Map<String,Object> value() throws NoSuchElementException {
        return debug;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public Iterator<Property> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public Object id() {
        return vertex.id().toString() + ":" + FireflyElement.DEBUG_STORAGE_PROPERTY;
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw new UnsupportedOperationException("not writeable");
    }


}
