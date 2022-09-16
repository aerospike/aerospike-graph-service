package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import groovy.util.MapEntry;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class PackedVertex extends RelationalVertex {
    private static final Logger LOG = LoggerFactory.getLogger(PackedVertex.class);
    public static final int VERTEX_TYPE_HINT = 1;

    private Map<String, Long> vertexPropertyIds;
    private Map<String, Object> vertexPropertyValues;
    private Map<String, Long> vertexPropertyValuesTypeHints;
    private long vertexPropertyCount;
    private AerospikeConnection db;

    /**
     * Constructor for PackedVertex.
     *
     * @param fid                  firefly id.
     * @param label                label.
     * @param graph                graph.
     * @param inEdgeIds            incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds           outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount          incoming edge count.
     * @param outEdgeCount         outgoing edge count.
     * @param vertexPropertyIds    vertex property ids.
     * @param vertexPropertyValues vertex property values.
     * @param vertexPropertyCount  vertex property count.
     * @param db                   Aerospike connection.
     */
    public PackedVertex(final FireflyId fid,
                        final String label,
                        final FireflyGraph graph,
                        final Map<String, List<Long>> inEdgeIds,
                        final Map<String, List<Long>> outEdgeIds,
                        final long inEdgeCount,
                        final long outEdgeCount,
                        final Map<String, Long> vertexPropertyIds,
                        final Map<String, Object> vertexPropertyValues,
                        final Map<String, Long> vertexPropertyValuesTypeHints,
                        final long vertexPropertyCount,
                        final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, db);

        // To enable values to have index functions run, cardinality must be single.
        if (graph().features().vertex().getCardinality("") != VertexProperty.Cardinality.single) {
            throw new RuntimeException("PackedVertex only supports for single cardinality");
        }

        this.vertexPropertyCount = vertexPropertyCount;
        this.vertexPropertyIds = vertexPropertyIds == null ? new HashMap<>() : vertexPropertyIds;
        this.vertexPropertyValues = vertexPropertyIds == null ? new HashMap<>() : vertexPropertyValues;
        this.vertexPropertyValuesTypeHints = vertexPropertyIds == null ? new HashMap<>() : vertexPropertyValuesTypeHints;
        this.db = db;
    }

    @Override
    protected void removeVertexProperties() {
        vertexPropertyIds = new HashMap<>();
        vertexPropertyValues = new HashMap<>();
        vertexPropertyValuesTypeHints = new HashMap<>();
    }

    /**
     * Read vertex properties for the vertex.
     *
     * @return Iterator of String label to List of FireflyVertexProperty
     */
    @Override
    protected <V> Iterator<Map.Entry<String, VertexProperty<V>>> readVertexProperties() {
        LOG.debug("Read vertex properties");

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final List<Map.Entry<String, FireflyVertexProperty<V>>> vertexPropertyList = new ArrayList<>();

        for (final Map.Entry<String, Object> vertexProperty : vertexPropertyValues.entrySet()) {
            // Create the property.
            final FireflyVertexProperty<V> property = new PackedVertexProperty<>(graph,
                    FireflyId.of(PackedVertexProperty.class, vertexPropertyIds.get(vertexProperty.getKey())),
                    this,
                    vertexProperty.getKey(),
                    vertexProperty.getValue());
            vertexPropertyList.add(new MapEntry(vertexProperty.getKey(), property));

        }

        return IteratorUtils.asIterator(vertexPropertyList);
    }

    /**
     * Get the vertex property by vertex property label for the vertex.
     *
     * @param key vertex property label.
     * @return Iterator of FireflyVertexProperty for provided vertex property label.
     */
    @Override
    protected <V> Iterator<VertexProperty<V>> readVertexProperty(final String key) {
        LOG.debug("Reading vertex property {}", key);

        if (!vertexPropertyValues.containsKey(key)) {
            return Collections.emptyIterator();
        }

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final Object vertexProperty = vertexPropertyValues.get(key);
        final Long vertexPropertyId = vertexPropertyIds.get(key);

        final List<VertexProperty<V>> vertexProperties = new ArrayList<>();
        vertexProperties.add(
                new PackedVertexProperty<>(graph,
                        FireflyId.of(PackedVertexProperty.class, vertexPropertyId),
                        this,
                        key,
                        vertexProperty));
        return IteratorUtils.asIterator(vertexProperties);
    }

    /**
     * Remove the vertex property from the vertex.
     *
     * @param key              vertex property label.
     * @param vertexPropertyId id of the vertex property to remove.
     */
    @Override
    public void removeVertexPropertyForModel(final String key, final FireflyId vertexPropertyId) {
        LOG.debug("Removing vertex property {} from vertex {}.", vertexPropertyId.value(), id.value());

        if (!vertexPropertyIds.containsKey(key)) {
            LOG.error("Could not find vertex property {} in vertex {}. Vertex properties did not contain key {}.",
                    vertexPropertyId.value(), id.value(), key);
            return;
        }

        // Remove vertex property from vertex properties in vertex.
        if (!vertexPropertyId.toNumericId().value().equals(vertexPropertyIds.get(key))) {
            LOG.error("Could not find vertex property {} in vertex {}. Vertex properties under key {} did not contain vertex property {}.",
                    vertexPropertyId.value(), id.value(), key, vertexPropertyId.value());
            return;
        }

        // Remove the vertex property from vertex property list.
        vertexPropertyIds.remove(key);
        vertexPropertyValues.remove(key);
        vertexPropertyValuesTypeHints.remove(key);
        vertexPropertyCount--;

        // Create vertex property related bins.
        final Bin vertexPropertiesValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexPropertyValues));
        final Bin vertexPropertiesIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIds));
        final Bin vertexPropertiesValuesTypeHintsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexPropertyValuesTypeHints));
        final Bin vertexPropertiesCounterBin = new Bin(db.VP_COUNTER, Value.get(vertexPropertyCount));

        // Write back to Aerospike.
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, vertexPropertiesValuesBin, vertexPropertiesIdsBin, vertexPropertiesValuesTypeHintsBin, vertexPropertiesCounterBin);
    }

    /**
     * Write vertex property to vertex.
     *
     * @param vertexProperty Vertex property to write to vertex.
     */
    @Override
    public void writeVertexProperty(final FireflyVertexProperty vertexProperty) {
        LOG.debug("Adding vertex property {} to vertex {}.", vertexProperty.id.value(), id.value());

        // Update maps for vertex properties and ids.
        vertexPropertyValues.put(vertexProperty.key(), vertexProperty.value());
        vertexPropertyIds.put(vertexProperty.key(), (Long) vertexProperty.id.toNumericId().value());
        vertexPropertyValuesTypeHints.put(vertexProperty.key(), db.getSupportedType(vertexProperty.value().getClass()));
        vertexPropertyCount++;

        // Create vertex property related bins.
        final Bin vertexPropertiesValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexPropertyValues));
        final Bin vertexPropertiesValuesTypeHintBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, vertexPropertyValuesTypeHints);
        final Bin vertexPropertiesIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIds));
        final Bin vertexPropertiesCounterBin = new Bin(db.VP_COUNTER, Value.get(vertexPropertyCount));

        // Write back to Aerospike.
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, vertexPropertiesValuesBin, vertexPropertiesIdsBin, vertexPropertiesValuesTypeHintBin, vertexPropertiesCounterBin);
    }

    /**
     * Get vertex property count.
     *
     * @return Vertex property count.
     */
    @Override
    public long getVertexPropertyCount() {
        return vertexPropertyCount;
    }

    /**
     * Read vertex property keys.
     *
     * @return Set of vertex property keys.
     */
    @Override
    protected Set<String> readVertexPropertyKeys() {
        return vertexPropertyValues.keySet();
    }
}
