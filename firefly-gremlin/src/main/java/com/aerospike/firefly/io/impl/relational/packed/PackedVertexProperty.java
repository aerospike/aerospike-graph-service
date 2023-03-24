package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
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
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalProperty;
import com.aerospike.firefly.io.utils.ElementNotFoundException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedTypeValues;
import static com.aerospike.firefly.io.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
final public class PackedVertexProperty<V> extends FireflyVertexProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(PackedVertexProperty.class);
    private final PackedVertex vertex;

    /**
     * Constructor for PackedVertexProperty.
     *
     * @param graph  Graph that vertex property exists on.
     * @param id     Id of vertex property.
     * @param vertex Vertex.
     * @param key    Key of vertex property.
     * @param value  Value of vertex property.
     */
    public PackedVertexProperty(final FireflyGraph graph,
                                final FireflyId id,
                                final PackedVertex vertex,
                                final String key,
                                final Object value) {
        super(graph, id, vertex.id, key, (V) value);
        this.vertex = vertex;
    }

    /**
     * Additional private constructor for creating a property from a record.
     *
     * @param graph    Graph that vertex property exists on.
     * @param id       Id of vertex property.
     * @param vertexId Id of vertex.
     * @param key      Key of vertex property.
     * @param value    Value of vertex property.
     */
    private PackedVertexProperty(final FireflyGraph graph,
                                 final FireflyId id,
                                 final FireflyId vertexId,
                                 final String key,
                                 final Object value) {
        super(graph, id, vertexId, key, (V) value);
        this.vertex = null;
    }

    /**
     * Create vertex property from a record.
     *
     * @param graph         Graph that property exists on.
     * @param key           Key of property.
     * @param fireflyRecord Record to create vertex property from
     * @param parentId      Id of parent vertex.
     * @param <V>           Type of value.
     * @return The vertex property.
     */
    public static <V> FireflyVertexProperty<V> fromRecord(final FireflyGraph graph,
                                                          final String key,
                                                          final FireflyRecord fireflyRecord,
                                                          final FireflyId parentId) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Map<String, Object> propertyValueMap = (Map<String, Object>) fireflyRecord.record().getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
        final Map<String, Long> propertyValueTypeHintsMap = (Map<String, Long>) fireflyRecord.record().getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
        final Map<String, Long> propertyIdMap = (Map<String, Long>) fireflyRecord.record().getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
        propertyValueMap.replaceAll((k, v) -> db.convertValuetoTypeUsingHint(v, propertyValueTypeHintsMap.get(k)));
        final Object property = propertyValueMap.get(key);
        if (property == null) {
            // Cannot get id.
            return new PackedVertexProperty<>(graph, null, parentId, null, null);
        }
        return new PackedVertexProperty<>(graph, graph.getIdFactory().createId(propertyIdMap.get(key), FireflyVertexProperty.class), parentId, key, propertyValueMap.get(key));
    }

    /**
     * Write a new vertex property
     *
     * @param vertex parent Vertex
     * @param vpid   VertexProperty id to write
     * @param key    VP key
     * @param value  VP value
     * @param <V>    type
     */
    public static <V> FireflyVertexProperty<V> writeVertexProperty(
            final FireflyGraph graph,
            final FireflyVertex vertex,
            final FireflyId vpid,
            final String key,
            final V value) {
        // Do nothing.

        // Return the vertex property.
        return new PackedVertexProperty<>(graph, vpid, (PackedVertex) vertex, key, value);
    }

    /**
     * Remove a vertex property.
     *
     * @param graph Graph to remove vertex property from.
     * @param id    Id of vertex property.
     */
    public static void removeVertexProperty(final FireflyGraph graph, final FireflyId id) {
        LOG.debug("Removing vertex property {}", id);

        // Do nothing.
    }

    /**
     * Remove this vertex property.
     */
    @Override
    public void remove() {
        try {
            LOG.debug("Removing vertex property {}", id);
            if (vertex == null) {
                graph.readVertex(vertexId).removeVertexProperty(label, id);
            } else {
                vertex.removeVertexProperty(label, id);
            }
            removeVertexProperty(graph, id);
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
     * Get the FireflyId of the parent vertex to which this vertex property belongs.
     *
     * @return The FireflyId of the parent vertex.
     */
    public FireflyId getVertexId() {
        return this.vertexId;
    }

    /**
     * Write a property to this vertex property.
     *
     * @param key   The key of the property to write.
     * @param value The value of the property to write.
     */
    public void writeProperty(final String key, final Object value) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final Key opKey = getKey(db, db.VERTEX_AERO_SET, this.getVertexId());

        final Operation writeValue;
        final Operation writeTypeHint;

        if (value == null) {
            writeValue = MapOperation.removeByKey(db.PROPERTIES, Value.get(key), MapReturnType.NONE,
                    CTX.mapKey(Value.get(this.id.getStorageId())));
            writeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS, Value.get(key), MapReturnType.NONE,
                    CTX.mapKey(Value.get(this.id.getStorageId())));
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            writeValue = MapOperation.put(policy, db.PROPERTIES, Value.get(key), Value.get(value),
                    CTX.mapKey(Value.get(this.id.getStorageId())));
            writeTypeHint = MapOperation.put(policy, db.TYPE_HINTS, Value.get(key),
                    Value.get(getSupportedType(value.getClass())),
                    CTX.mapKey(Value.get(this.id.getStorageId())));
        }

        final FireflyCache cache = this.graph.getBaseGraph().transactionCache.get();
        if (cache != null) {
            cache.invalidate(opKey);
        }
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.operate(writePolicy, opKey, writeValue, writeTypeHint);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Vertex Property has been removed from the Vertex since in this case
                // the key is the Vertex key due to Vertex Properties being packed and thus the key still exists.
                throw new ElementNotFoundException(this, ae);
            } else {
                throw ae;
            }
        }
    }

    /**
     * Remove a property from this vertex property.
     *
     * @param key The key of the property to be removed.
     */
    public void removeProperty(final String key) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final Key opKey = getKey(db, db.VERTEX_AERO_SET, this.getVertexId());

        final Operation removeProperty = MapOperation.removeByKey(db.PROPERTIES, Value.get(key), MapReturnType.NONE,
                CTX.mapKey(Value.get(this.id.getStorageId())));
        final Operation removeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS, Value.get(key), MapReturnType.NONE,
                CTX.mapKey(Value.get(this.id.getStorageId())));

        final FireflyCache cache = this.graph.getBaseGraph().transactionCache.get();
        if (cache != null) {
            cache.invalidate(opKey);
        }
        try {
            db.operate(null, opKey, removeProperty, removeTypeHint);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Vertex Property has been removed from the Vertex since in this case
                // the key is the Vertex key due to Vertex Properties being packed and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed vertex property {}", this, ae);
            } else {
                throw ae;
            }
        }
    }

    /**
     * Read all the properties of this vertex property.
     *
     * @param <V> The value type of a property.
     * @return A map of all the key value pairs of properties on this vertex property.
     */
    public <V> Map<String, Property<V>> readProperties() {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, getVertexId());
        final Map<String, Property<V>> returnedProperties = new HashMap<>();
        if (fireflyRecord == null) {
            LOG.error("Could not retrieve properties of vertex property - missing parent vertex ID: " + getVertexId().getUserId());
            return returnedProperties;
        }

        final Record r = fireflyRecord.record();
        final Map<Object, Map<String, Object>> properties =
                (Map<Object, Map<String, Object>>) Optional.ofNullable(r.getMap(db.PROPERTIES)).orElse(new TreeMap<>());
        final Map<Object, Map<String, Object>> typeHints =
                (Map<Object, Map<String, Object>>) Optional.ofNullable(r.getMap(db.TYPE_HINTS)).orElse(new TreeMap<>());

        if (!properties.containsKey(this.id.getStorageId()) || !typeHints.containsKey(this.id.getStorageId())) {
            return returnedProperties;
        } else {
            final Map<String, Object> propertiesForVp = properties.get(this.id.getStorageId());
            final Map<String, Object> typeHintsForVp = typeHints.get(this.id.getStorageId());
            for (final Map.Entry<String, Object> propertyEntry : propertiesForVp.entrySet()) {
                if (!typeHintsForVp.containsKey(propertyEntry.getKey())) {
                    LOG.warn("Missing type hint for property key " + propertyEntry.getKey());
                } else {
                    final Long typeHint = (Long) typeHintsForVp.get(propertyEntry.getKey());
                    final Class valueClass = SupportedTypeValues.get(typeHint);
                    final V value = (V) db.typeCast(valueClass, propertyEntry.getValue());
                    final Property<V> property = new RelationalProperty<>(graph, this, propertyEntry.getKey(), value);
                    returnedProperties.put(propertyEntry.getKey(), property);
                }
            }
            return returnedProperties;
        }
    }
}
