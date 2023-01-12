package com.aerospike.firefly.io.impl.relational.linked;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
final public class LinkedVertexProperty<V> extends FireflyVertexProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedVertexProperty.class);
    private final LinkedVertex vertex;

    /**
     * Constructor for LinkedVertexProperty.
     *
     * @param graph  Graph that vertex property exists on.
     * @param id     Id of vertex property.
     * @param vertex Vertex.
     * @param key    Key of vertex property.
     * @param value  Value of vertex property.
     */
    public LinkedVertexProperty(final FireflyGraph graph,
                                final FireflyId id,
                                final LinkedVertex vertex,
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
    private LinkedVertexProperty(final FireflyGraph graph,
                                 final FireflyId id,
                                 final FireflyId vertexId,
                                 final String key,
                                 final Object value) {
        super(graph, id, vertexId, key, (V) value);
        this.vertex = null;
    }

    /**
     * Read a single VertexProperty from its id
     *
     * @param graph  the graph to read from.
     * @param parent Vertex that owns the VertexProperty being looked up
     * @param id     the id of the VertexProperty to read.
     * @return VertexProperty
     */
    public static <V> FireflyVertexProperty<V> readVertexProperty(final FireflyGraph graph,
                                                                  final FireflyVertex parent,
                                                                  final FireflyId id) {
        LOG.debug("Reading vertex property {} from {}", id, parent.id);

        // Read record from Aerospike.
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_PROPERTY_AERO_SET, id);
        if (fireflyRecord == null) {
            return null;
        }

        // Read vertex property from record.
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(
                db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, id, db.KEY_VALUE, db.VP_TYPE_HINTS));

        // Return the vertex property.
        return (kv.isEmpty()) ?
                new LinkedVertexProperty<>(graph, id, (LinkedVertex) parent, null, null) :
                new LinkedVertexProperty<>(graph, id, (LinkedVertex) parent, kv.get().getKey(), kv.get().getValue());
    }

    /**
     * Create vertex property from a record.
     *
     * @param graph         Graph that property exists on.
     * @param fireflyRecord Record to create vertex property from
     * @param parentId      Id of parent vertex.
     * @param <V>           Type of value.
     * @return The vertex property.
     */
    public static <V> FireflyVertexProperty<V> fromRecord(final FireflyGraph graph, final FireflyRecord fireflyRecord, final FireflyId parentId) {
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyId fid = FireflyIdFactory.createId(fireflyRecord.id());
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(
                db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, fid, db.KEY_VALUE, db.VP_TYPE_HINTS));
        if (kv.isEmpty()) {
            return null;
        }
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new LinkedVertexProperty<>(graph, fid, parentId, vpKey, vpVal);
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
        // Write the vertex property to the database
        final AerospikeConnection db = graph.getBaseGraph();
        final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, Value.get(key));
        final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, Value.get(vertex.id.getStorageId()));
        db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vpid, db.KEY_VALUE, key, value, db.VP_TYPE_HINTS,
                vpkBin, pviBin);
        // Return the vertex property.
        return new LinkedVertexProperty<>(graph, vpid, (LinkedVertex) vertex, key, value);
    }

    /**
     * Remove a vertex property.
     *
     * @param graph Graph to remove vertex property from.
     * @param id    Id of vertex property.
     */
    public static void removeVertexProperty(final FireflyGraph graph, final FireflyId id) {
        LOG.debug("Removing vertex property {}", id);
        final AerospikeConnection db = graph.getBaseGraph();
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.VERTEX_PROPERTY_AERO_SET, id));
    }

    /**
     * Remove this vertex property.
     */
    @Override
    public void remove() {
        try {
            // Remove vertex property from vertex first so if we fail it will null out.
            LOG.info("Removing vertex property {}", this.id);
            removeVertexProperty(this.graph, this.id);
            if (this.vertex == null) {
                this.graph.readVertex(this.vertexId).removeVertexProperty(this.label, this.id);
            } else {
                this.vertex.removeVertexProperty(this.label, this.id);
            }
        } catch (final AerospikeException e) {
            // Removing a vertex property that is already removed SHOULD NOT yield an error.
            LOG.debug("Ignoring caught exception when removing vertex property " + this.id, e);
        }
    }
}
