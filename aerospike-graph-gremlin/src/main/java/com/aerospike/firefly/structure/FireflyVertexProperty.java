package com.aerospike.firefly.structure;

import com.aerospike.client.ResultCode;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexProperty.class);
    protected final boolean allowNullPropertyValues = false;
    public final FireflyId vertexId;
    protected final String key;
    protected final V value;
    protected final FireflyGraph graph;
    protected FireflyVertex vertex;
    public Map<String, Object> properties;
    public Map<String, Object> typeHints;

    public FireflyVertexProperty(final FireflyGraph graph,
                                 final FireflyId id,
                                 final FireflyVertex vertex,
                                 final String key,
                                 final V value,
                                 final Map<String, Object> properties,
                                 final Map<String, Object> typeHints) {
        super(id, key);
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");
        this.graph = graph;
        this.vertexId = vertex.id;
        this.key = key;
        this.value = value;
        this.properties = properties == null ? new TreeMap<>() : properties;
        this.typeHints = typeHints == null ? new TreeMap<>() : typeHints;
        this.vertex = vertex;
    }

    public FireflyVertexProperty(final FireflyGraph graph, final FireflyId fid, final FireflyId vertexId, final String key, final V value, final FireflyVertex vertex) {
        super(fid, key);
        this.graph = graph;
        this.vertexId = vertexId;
        this.key = key;
        this.value = value;
        this.vertex = vertex;
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

    /**
     * Remove this vertex property.
     */
    @Override
    public void remove() {
        try {
            LOG.debug("Removing vertex property {}", id);
            if (vertex == null) {
                vertex = graph.readVertex(vertexId);
            }
            if (vertex != null) {
                graph.aerospikeOperations.removeVertexProperty(vertex, label, id);
            }
        } catch (final AerospikeGraphException ae) {
            // Removing a property that is already removed SHOULD NOT yield an error.
            if (ae.errorCode == ResultCode.KEY_NOT_FOUND_ERROR) {
                LOG.debug("Ignored exception removing an already-removed vertex property {}.", this, ae);
            } else {
                throw ae;
            }
        }
    }

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
        if (vertex == null) {
            vertex = graph.readVertex(vertexId);
        }
        return vertex;
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

        final U validatedValue = (U) FireflyHelper.validatePropertyValue(value);
        return graph.aerospikeOperations.writeVpProperty(this, key, validatedValue);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        if (propertyKeys.length == 1) {
            if (!properties.containsKey(propertyKeys[0]) ||
                    (properties.get(propertyKeys[0]) == null &&
                            !graph.features().vertex().supportsNullPropertyValues())) {
                return Collections.emptyIterator();
            }
            final Property<V> property = new FireflyVertexPropertyProperty<>(
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
                    p -> new FireflyVertexPropertyProperty<>(graph, this, p.getKey(),
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

