package com.aerospike.firefly.io.impl.linked;

import com.aerospike.client.Bin;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class LinkedVertexProperty<V> extends FireflyVertexProperty<V> {

    /**
     * Constructor for LinkedVertexProperty.
     *
     * @param graph    Graph to use.
     * @param id       Id of vertex property.
     * @param vertexId Id of vertex.
     * @param key      Key of vertex property.
     * @param value    Value of vertex property.
     */
    public LinkedVertexProperty(final FireflyGraph graph,
                                final FireflyId id,
                                final FireflyId vertexId,
                                final String key,
                                final Object value) {
        super(graph, id, vertexId, key, (V) value);
    }

    /**
     * Read a single VertexProperty from its id
     *
     * @param graph  the graph to read from.
     * @param parent Vertex that owns the VertexProperty being looked up
     * @param id     the id of the VertexProperty to read.
     * @return VertexProperty
     */
    public static <V> FireflyVertexProperty<V> readVertexProperty(FireflyGraph graph, FireflyVertex parent, FireflyId id) {
        // Read record from Aerospike.
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_PROPERTY_AERO_SET, id);
        if (fireflyRecord == null)
            throw new NoSuchElementException();

        // Read vertex property from record.
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(
                db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, id, db.KEY_VALUE));

        // Return the vertex property.
        return (kv.isEmpty()) ?
                new LinkedVertexProperty<V>(graph, id, parent.id, null, null) :
                new LinkedVertexProperty<V>(graph, id, parent.id, kv.get().getKey(), kv.get().getValue());
    }

    public static <V> FireflyVertexProperty<V> fromRecord(FireflyGraph graph, FireflyRecord fireflyRecord, final FireflyId parentId) {
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyId fid = FireflyId.of(FireflyVertexProperty.class, fireflyRecord.id());
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, fid, db.KEY_VALUE));
        if (kv.isEmpty())
            return new LinkedVertexProperty<>(graph, fid, parentId, null, null);
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new LinkedVertexProperty<>(graph, fid, parentId, vpKey, (V) vpVal);
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
        final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, key);
        final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, db.idToStorageType(vertex.id()));
        db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vpid, db.KEY_VALUE, key, value, vpkBin, pviBin);

        // Return the vertex property.
        return new LinkedVertexProperty<>(graph, vpid, vertex.id, key, value);
    }

    public static void removeVertexProperty(FireflyGraph graph, FireflyId id) {
        final AerospikeConnection db = graph.getBaseGraph();
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.VERTEX_PROPERTY_AERO_SET, id));
    }

    @Override
    public void remove() {
        graph.readVertex(vertexId).removeVertexProperty(label, id);
        removeVertexProperty(graph, id);
    }
}
