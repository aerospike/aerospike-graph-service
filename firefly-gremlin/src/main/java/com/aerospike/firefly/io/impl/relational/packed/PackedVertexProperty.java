package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalProperty;
import com.aerospike.firefly.io.utils.GenerationCheck;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
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
        final Map<String, Object> propertyValueMap = (Map<String, Object>) fireflyRecord.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
        final Map<String, Long> propertyValueTypeHintsMap = (Map<String, Long>) fireflyRecord.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
        final Map<String, Long> propertyIdMap = (Map<String, Long>) fireflyRecord.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
        propertyValueMap.replaceAll((k, v) -> db.convertValuetoTypeUsingHint(v, propertyValueTypeHintsMap.get(k)));
        final Object property = propertyValueMap.get(key);
        if (property == null) {
            // Cannot get id.
            return new PackedVertexProperty<>(graph, null, parentId, null, null);
        }
        return new PackedVertexProperty<>(graph, FireflyIdFactory.createId(propertyIdMap.get(key)), parentId, key, propertyValueMap.get(key));
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
        } catch (Exception ignored) {
            // Removing a vertex property that is already removed SHOULD NOT yield an error.
        }
    }

    /**
     * Get the FireflyId of the parent vertex to which this vertex property belongs.
     *
     * @return  The FireflyId of the parent vertex.
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
        GenerationCheck.writeGenerationCheck(() -> protectedWriteProperty(key, value));
    }

    private void protectedWriteProperty(final String key, final Object value) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final Map<Object, Map<String, Object>> properties;
        final Map<Object, Map<String, Object>> typeHints;
        final int generation;

        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, getVertexId());
        if (fireflyRecord == null) {
            LOG.error("Attempted to write a property to vertex property with a non-existent parent vertex: " + getVertexId().getUserId());
            return;
        } else {
            generation = fireflyRecord.record.generation;
            properties = (Map<Object, Map<String, Object>>) Optional.ofNullable(fireflyRecord.record.getMap(db.PROPERTIES)).orElse(new TreeMap<>());
            typeHints = (Map<Object, Map<String, Object>>) Optional.ofNullable(fireflyRecord.record.getMap(db.TYPE_HINTS)).orElse(new TreeMap<>());
        }

        if (!typeHints.containsKey(this.id.getStorageId())) {
            typeHints.put(this.id.getStorageId(), new TreeMap<>());
        }
        if (!properties.containsKey(this.id.getStorageId())) {
            properties.put(this.id.getStorageId(), new TreeMap<>());
        }
        final Map<String, Object> typeHintsForVp = typeHints.get(this.id.getStorageId());
        final Map<String, Object> propertiesForVp = properties.get(id.getStorageId());

        if (value == null) {
            propertiesForVp.remove(key);
            typeHintsForVp.remove(key);
        } else {
            typeHintsForVp.put(key, getSupportedType(value.getClass()));
            propertiesForVp.put(key, value);
        }

        final Bin typeHintBin = new Bin(db.TYPE_HINTS, Value.get(typeHints, MapOrder.KEY_ORDERED));
        final Bin propertiesBin = new Bin(db.PROPERTIES, Value.get(properties, MapOrder.KEY_ORDERED));

        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, getVertexId(), generation, typeHintBin, propertiesBin);
    }

    /**
     * Remove a property from this vertex property.
     *
     * @param key   The key of the property to be removed.
     */
    public void removeProperty(final String key) {
        GenerationCheck.writeGenerationCheck(() -> protectedRemoveProperty(key));
    }

    private void protectedRemoveProperty(final String key) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, getVertexId());
        if (fireflyRecord == null)
            return;
        final Record r = fireflyRecord.record;
        final int generation = r.generation;
        final Map<Object, Map<String, Object>> properties =
                (Map<Object, Map<String, Object>>) Optional.ofNullable(r.getMap(db.PROPERTIES)).orElse(new TreeMap<>());
        final Map<Object, Map<String, Object>> typeHints =
                (Map<Object, Map<String, Object>>) Optional.ofNullable(r.getMap(db.TYPE_HINTS)).orElse(new TreeMap<>());

        if (properties.containsKey(this.id.getStorageId())) {
            final Map<String, Object> propertiesForVp = properties.get(this.id.getStorageId());
            propertiesForVp.remove(key);
        }
        if (typeHints.containsKey(this.id.getStorageId())) {
            final Map<String, Object> typeHintsForVp = typeHints.get(this.id.getStorageId());
            typeHintsForVp.remove(key);
        }
        final Bin typeHintBin = new Bin(db.TYPE_HINTS, Value.get(typeHints, MapOrder.KEY_ORDERED));
        final Bin propertiesBin = new Bin(db.PROPERTIES, Value.get(properties, MapOrder.KEY_ORDERED));

        FireflyRecord.write(db, db.VERTEX_AERO_SET, getVertexId(), generation, propertiesBin, typeHintBin);
    }

    /**
     * Read all the properties of this vertex property.
     *
     * @return      A map of all the key value pairs of properties on this vertex property.
     * @param <V>   The value type of a property.
     */
    public <V> Map<String, Property<V>> readProperties() {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, getVertexId());
        final Map<String, Property<V>> returnedProperties = new HashMap<>();
        if (fireflyRecord == null) {
            LOG.error("Could not retrieve properties of vertex property - missing parent vertex ID: " + getVertexId().getUserId());
            return returnedProperties;
        }

        final Record r = fireflyRecord.record;
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
