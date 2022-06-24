package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    private final boolean allowNullPropertyValues = true;
    private final FireflyVertex vertex;
    private final String key;
    private final V value;
    private final FireflyGraph graph;

    private Map<String, Property> readProperties() {
        return ((FireflyGraph) this.graph()).getBaseGraph().readProperties(this);
    }

    private void writeProperty(String k, Object v) {
        ((FireflyGraph) this.graph()).getBaseGraph().writeProperty(this.id, this.getClass(), k, v);
    }


    public FireflyVertexProperty(final FireflyGraph graph, final FireflyId id, final FireflyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key);
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");
        this.graph = graph;
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public FireflyVertexProperty(final FireflyGraph graph, final FireflyId fid, final FireflyVertex vertex, String key, V value) {
        super(fid, key);
        this.graph = graph;
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");

        this.vertex = vertex;
        this.key = key;
        this.value = value;

    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
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
    public <U> Property<U> property(String key, U value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);

        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }

        this.writeProperty(key, value);
        return this.readProperties().get(key);
    }

    @Override
    public void remove() {
        try {
            ((FireflyGraph) this.graph()).getBaseGraph().removeVertexProperty((FireflyGraph) this.graph(), this);
        } catch (AerospikeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Map<String, Property> properties = this.readProperties();
        if (propertyKeys.length == 1) {
            final Property<V> property = properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else {
            return IteratorUtils.map(IteratorUtils.filter(IteratorUtils.asIterator(properties.entrySet()),
                    entry -> ElementHelper.keyExists((String) ((AbstractMap.Entry) entry).getKey(), propertyKeys)), entry ->
                    ((AbstractMap.Entry) entry).getValue());
        }
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        boolean areEqual = ElementHelper.areEqual(this, object);
        return areEqual;
    }

    @Override
    public Record getBaseElement() {
        return FireflyRecord.read(graph.getBaseGraph(),graph.getBaseGraph().VERTEX_PROPERTY_AERO_SET,FireflyId.fromElement(this)).record();
    }
}

