package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.runtime.exceptions.ElementNotFoundException;
import com.aerospike.firefly.runtime.exceptions.RecordTooBigException;
import com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException.fromAddingVpProperty;
import static com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException.getRelevantVertexBins;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexProperty.class);
    protected final boolean allowNullPropertyValues = false;
    protected final FireflyId vertexId;
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
            if (vertex == null) {
                throw new IllegalStateException("Failed to remove VertexProperty with id " + id.getUserId() +
                        " from vertex with id " + vertexId.getUserId() + " because the vertex does not exist.");
            }
            vertex.removeVertexProperty(label, id);
        } catch (final AerospikeException ae) {
            // Removing a property that is already removed SHOULD NOT yield an error.
            if (ae.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                LOG.debug("Ignored exception removing an already-removed vertex property {}.", this, ae);
            } else {
                throw ae;
            }
        }
    }

    /**
     * Write properties to element.
     *
     * @param propertyKey    Key of property to write.
     * @param propertyValue  Value of property to write.
     * @param <F>            Type of property value.
     * @return Map of label to properties.
     */
    public <F> Property<F> writeProperty(final String propertyKey, final F propertyValue) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key opKey = getKey(db, db.VERTEX_AERO_SET, vertexId);
        final List<Operation> operations = new ArrayList<>();

        final Operation writeValue;
        if (propertyValue == null) {
            writeValue = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(Value.get(id.getStorageId())));
            operations.add(writeValue);
            final Operation writeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(Value.get(id.getStorageId())));
            operations.add(writeTypeHint);
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            writeValue = MapOperation.put(policy, db.PROPERTIES_BIN, Value.get(propertyKey), Value.get(propertyValue),
                    CTX.mapKey(Value.get(id.getStorageId())));
            operations.add(writeValue);
            final Object typeHint = Value.get(getTypeHintOf(propertyValue));
            if (typeHint != null) {
                final Operation writeTypeHint = MapOperation.put(policy, db.TYPE_HINTS_BIN, Value.get(propertyKey),
                        Value.get(typeHint),
                        CTX.mapKey(Value.get(id.getStorageId())));
                operations.add(writeTypeHint);
            }
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.operate(writePolicy, opKey, operations.toArray(new Operation[0]));
        } catch (final RecordTooBigException rtbe) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVpProperty((AerospikeException) rtbe.getCause(), db, getRelevantVertexBins(db, opKey),
                    this.vertexId, this.key, propertyKey);
            FireflyVertexProperty.LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        } catch (final AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Vertex Property has been removed from the Vertex since in this case
                // the key is the Vertex key due to Vertex Properties being packed and thus the key still exists.
                throw new ElementNotFoundException(this, ae);
            } else {
                throw ae;
            }
        }
        this.properties.put(propertyKey, propertyValue);
        if (propertyValue == null || getTypeHintOf(propertyValue) == null) {
            this.typeHints.remove(propertyKey);
        } else {
            this.typeHints.put(propertyKey, getTypeHintOf(propertyValue));
        }
        return new FireflyVertexPropertyProperty<>(graph, this, propertyKey, propertyValue);
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
    public long getTtlMillis() {
        // This should never happen since this method is never invoked on a Vertex Property.
        throw new IllegalStateException("TTL is not supported for Vertex Properties");
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

