package com.aerospike.firefly.io.impl.relational.packed;

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
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.utils.ElementNotFoundException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedValueTypes;
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
                                final Object value,
                                final Map<String, Object> properties,
                                final Map<String, Object> typeHints) {
        super(graph, id, vertex.id, key, (V) value, properties, typeHints);
        this.vertex = vertex;
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

        final Operation writeValue;
        final Operation writeTypeHint;

        if (propertyValue == null) {
            writeValue = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(Value.get(id.getStorageId())));
            writeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(Value.get(id.getStorageId())));
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            writeValue = MapOperation.put(policy, db.PROPERTIES_BIN, Value.get(propertyKey), Value.get(propertyValue),
                    CTX.mapKey(Value.get(id.getStorageId())));
            writeTypeHint = MapOperation.put(policy, db.TYPE_HINTS_BIN, Value.get(propertyKey),
                    Value.get(getSupportedType(propertyValue)),
                    CTX.mapKey(Value.get(id.getStorageId())));
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
        this.properties.put(propertyKey, propertyValue);
        this.typeHints.put(propertyKey, propertyValue != null ? getSupportedType(propertyValue) : SupportedValueTypes.get(String.class));
        return new PackedVertexPropertyProperty<>(graph, this, propertyKey, propertyValue);
    }
}
