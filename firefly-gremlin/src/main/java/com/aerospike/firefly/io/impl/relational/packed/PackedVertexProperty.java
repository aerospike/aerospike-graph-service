package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.Bin;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
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
        return new PackedVertexProperty<>(graph, FireflyId.of(FireflyVertexProperty.class, propertyIdMap.get(key)), parentId, key, propertyValueMap.get(key));
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
        LOG.debug("Removing vertex property {}", id.value());

        // Do nothing.
    }

    /**
     * Remove this vertex property.
     */
    @Override
    public void remove() {
        try {
            LOG.info("Removing vertex property {}", id.value());
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
}
