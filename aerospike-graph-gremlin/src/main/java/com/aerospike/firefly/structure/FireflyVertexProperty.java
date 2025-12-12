package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphElementNotFoundException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexProperty.class);
    public final FireflyId vertexId;
    protected final String key;
    protected final V value;
    protected final FireflyGraph graph;
    protected FireflyVertex vertex;
    public Map<Long, List<Object>> properties;

    public FireflyVertexProperty(final FireflyGraph graph,
                                 final FireflyId id,
                                 final FireflyVertex vertex,
                                 final String key,
                                 final V value,
                                 final Map<Long, List<Object>> properties) {
        super(id, key);
        this.graph = graph;
        this.vertexId = vertex.id;
        this.key = key;
        this.value = value;
        this.properties = properties;
        this.vertex = vertex;
    }

    /**
     * Remove property from vertex property property cache.
     *
     * @param key Key to remove.
     */
    public void removePropertyFromCache(final String key) {
        properties.remove(graph.getBaseGraph().schemaManager.getVpPropertyRead(key));
    }

    /**
     * Remove this vertex property.
     */
    @Override
    public void remove() {
        LOG.debug("Removing vertex property {}", id);
        if (vertex == null) {
            vertex = graph.readVertex(vertexId);
        }
        if (vertex != null) {
            graph.aerospikeOperations.removeVertexProperty(vertex, key, value, id);
            this.removed = true;
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

        if (null == value) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }

        final U validatedValue = (U) FireflyHelper.validatePropertyValue(value);
        return graph.aerospikeOperations.writeVpProperty(this, key, validatedValue);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        if (this.removed) {
            throw elementAlreadyRemoved(VertexProperty.class, id);
        }
        if (propertyKeys == null) {
            return Collections.emptyIterator();
        }
        final String[] propertyKeysToRead;
        if (propertyKeys.length == 0) {
            propertyKeysToRead = properties.keySet().stream().
                    map(graph.getBaseGraph().schemaManager::getVpPropertyString).
                    toArray(String[]::new);
        } else {
            propertyKeysToRead = propertyKeys;
        }

        final AerospikeConnection db = this.graph.getBaseGraph();
        final List<Property<V>> propertyList = new ArrayList<>();
        for (final String key : propertyKeysToRead) {
            final Long schemaKey = db.schemaManager.getVpPropertyRead(key);
            if (this.properties.containsKey(schemaKey)) {
                final List<Object> valueAndTypeHint = this.properties.get(schemaKey);
                final Object convertedValue = db.convertValueToTypeUsingHint(valueAndTypeHint.get(0),
                        valueAndTypeHint.get(1));
                final Property<V> property = new FireflyVertexPropertyProperty<>(graph, this, key, (V) convertedValue);
                propertyList.add(property);
            }
        }
        return propertyList.iterator();
    }

    /**
     * Special function for appending properties to this Vertex Property when it is returned as a pre-existing value
     * when attempting to write using Cardinality.set and with Vertex Property Properties.
     *
     * @param properties Vertex Property Properties in on-disk format to add to this Vertex Property.
     */
    public void appendProperties(final Map<Long, List<Object>> properties) {
        if (properties.isEmpty()) {
            return;
        }
        try {
            this.graph.aerospikeOperations.appendVpProperties(this, properties);
        } catch (final AerospikeGraphElementNotFoundException e) {
            // We catch and ignore this because it only happens if the VP that matched the Set was concurrently removed
            // before we appended its properties as a separate write. In this case, we emulate the atomic behavior of
            // the properties() step by updating it in JVM and returning it as if updating the VP properties was
            // a success even though the VP was removed.
            LOG.debug("Appending VP Properties to VP with key {} and ID {} failed due to concurrent removal.", this.key, this.id);
        }
        this.properties.putAll(properties);
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
