package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.io.FireflyRecord;
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

    private Map<String, Property> readProperties() {
        return ((FireflyGraph) this.graph()).db.readProperties(this);
    }

    private void writeProperty(String k, Object v) {
        ((FireflyGraph) this.graph()).db.writeProperty(this.id,this.getClass(), k, v);
    }


    public FireflyVertexProperty(FireflyRecord record, final FireflyId id, final FireflyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key, record);
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");

        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public FireflyVertexProperty(FireflyRecord record, final FireflyId fid, final FireflyVertex vertex, String key, V value) {
        super(fid, key, record);
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
            ((FireflyGraph) this.graph()).db.removeVertexProperty(this);
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
        } else{
            return IteratorUtils.map(IteratorUtils.filter(IteratorUtils.asIterator(properties.entrySet()),
                    entry -> ElementHelper.keyExists((String)((AbstractMap.Entry) entry).getKey(), propertyKeys)),entry ->
                    ((AbstractMap.Entry)entry).getValue());
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
}

