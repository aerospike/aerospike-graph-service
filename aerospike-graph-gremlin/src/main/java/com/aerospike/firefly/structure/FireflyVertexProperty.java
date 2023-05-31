package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.impl.relational.packed.PackedVertexPropertyProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    protected final boolean allowNullPropertyValues = false;
    protected final FireflyId vertexId;
    protected final String key;
    protected final V value;
    protected final FireflyGraph graph;
    public Map<String, Object> properties;
    public Map<String, Object> typeHints;

    public FireflyVertexProperty(final FireflyGraph graph,
                                 final FireflyId id,
                                 final FireflyId vertexId,
                                 final String key,
                                 final V value,
                                 final Map<String, Object> properties,
                                 final Map<String, Object> typeHints) {
        super(id, key);
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");
        this.graph = graph;
        this.vertexId = vertexId;
        this.key = key;
        this.value = value;
        this.properties = properties == null ? new TreeMap<>() : properties;
        this.typeHints = typeHints == null ? new TreeMap<>() : typeHints;
    }

    public FireflyVertexProperty(final FireflyGraph graph, final FireflyId fid, final FireflyId vertexId, final String key, final V value) {
        super(fid, key);
        this.graph = graph;
        this.vertexId = vertexId;
        this.key = key;
        this.value = value;
    }

    /**
     * Remove property from vertex property property cache.
     *
     * @param key Key to remove.
     */
    public void removePropertyFromCache(final String key) {
        properties.remove(key);
        typeHints.remove(key);
    }

    public abstract <F> Property<F> writeProperty(final String propertyKey, final F propertyValue);

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return graph.readVertex(vertexId);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (this.removed) {
            throw elementAlreadyRemoved(VertexProperty.class, id);
        }

        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }

        FireflyHelper.validatePropertyValue(value);
        return writeProperty(key, value);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        if (propertyKeys.length == 1) {
            if (!properties.containsKey(propertyKeys[0]) ||
                    (properties.get(propertyKeys[0]) == null &&
                            !graph.features().vertex().supportsNullPropertyValues())) {
                return Collections.emptyIterator();
            }
            final Property<V> property = new PackedVertexPropertyProperty<>(
                    graph, this,
                    propertyKeys[0],
                    (V) this.graph.getBaseGraph().convertValuetoTypeUsingHint(properties.get(propertyKeys[0]), typeHints.get(propertyKeys[0])));
            return FireflyCloseableIteratorUtils.of(property);
        } else {
            final Map<String, Object> outputProperties = new HashMap<>(properties);
            if (!graph.features().vertex().supportsNullPropertyValues()) {
                outputProperties.entrySet().removeIf(entry -> entry.getValue() == null);
            }
            if (propertyKeys.length > 0) {
                outputProperties.entrySet().removeIf(entry -> !ElementHelper.keyExists(entry.getKey(), propertyKeys));
            }
            return FireflyCloseableIteratorUtils.map(outputProperties.entrySet().iterator(),
                    p -> new PackedVertexPropertyProperty<>(graph, this, p.getKey(),
                            (V) this.graph.getBaseGraph().convertValuetoTypeUsingHint(p.getValue(), typeHints.get(p.getKey()))));
        }
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}

