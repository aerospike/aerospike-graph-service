package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    private final boolean allowNullPropertyValues = true;
    private final FireflyVertex vertex;
    private final String key;
    private final V value;

    private Property<V> readProperty(String key) {
        return ((FireflyGraph) this.graph()).db.readProperty(this, key);
    }

    private Map<String, Property> readProperties() {
        return ((FireflyGraph) this.graph()).db.readProperties(this);
    }

    private void writeProperty(String key, Property property) {
        ((FireflyGraph) this.graph()).db.writeProperty(this, key, property);
    }

    protected FireflyVertexProperty(final Object id, final FireflyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key);
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");

        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
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

        final Property<U> property = new FireflyProperty<>(this, key, value);
        this.writeProperty(key, property);
        return property;
    }

    @Override
    public void remove() {

    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        Map<String, Property> properties = this.readProperties();
        if (propertyKeys.length == 1) {
            final Property<U> property = properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }

    @Override
    public void removeProperty(String key) {
        ((FireflyGraph)this.graph()).db.removePropertyFromVertexProperty(this,key);
    }
    @Override
    public String toString(){
        return StringFactory.propertyString(this);
    }
}
