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
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.packed.PackedEdgeProperty;
import com.aerospike.firefly.io.utils.ElementNotFoundException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.io.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyEdge extends FireflyElement implements Edge {
    public boolean removed;
    protected final FireflyGraph graph;
    protected final FireflyId inVid;
    protected final FireflyId outVid;
    protected final Map<String, Object> properties;
    protected final Map<String, Object> typeHints;

    public abstract void removeEdge();

    public abstract void removePropertyFromCache(final String key);

    public FireflyEdge(final FireflyId id,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId inVid,
                       final FireflyId outVid,
                       final Map<String, Object> properties,
                       final Map<String, Object> typeHints) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
        this.properties = properties;
        this.typeHints = typeHints;
    }

    @Override
    public Vertex outVertex() {
        return graph.readVertex(this.outVid);
    }

    @Override
    public Vertex inVertex() {
        return graph.readVertex(this.inVid);
    }

    public FireflyId outVertexId() {
        return this.outVid;
    }

    public FireflyId inVertexId() {
        return this.inVid;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return FireflyCloseableIteratorUtils.of(this.outVertex());
            case IN:
                return FireflyCloseableIteratorUtils.of(this.inVertex());
            default:
                return FireflyCloseableIteratorUtils.of(this.outVertex(), this.inVertex());
        }
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Property<V> property(final String key) {
        if (properties.containsKey(key)) {
            final V casted = (V) this.graph.getBaseGraph().convertValuetoTypeUsingHint(properties.get(key), typeHints.get(key));
            return new PackedEdgeProperty<>(graph, this, key, casted);
        } else {
            return Property.empty();
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        FireflyHelper.legalPropertyKeyValueArray(key, value);

        // Cannot be hidden key.
        if (isHidden(key))
            throw Edge.Exceptions.labelCanNotBeAHiddenKey(key);

        // If edge is removed, cannot remove property.
        if (this.removed) {
            throw elementAlreadyRemoved(Edge.class, id);
        }

        // Remove the property.
        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            properties.remove(key);
            typeHints.remove(key);
            return Property.empty();
        }

        // Write the property and add to edge.
        FireflyHelper.validatePropertyValue(value);
        final Property<V> property = writeProperty(graph, this, key, value);
        properties.put(key, value);
        typeHints.put(key, AerospikeConnection.getSupportedType(value));
        return property;
    }

    @Override
    public void remove() {
        //@todo multi record transactions
        // Until we have MRT support, we must remove the edge record itself first, then
        // remove the edge from the individual vertices.
        // But doing this, should one of the subsequent deletes fail, we will not have an
        // orphaned edge on one vertex but not the other.
        removeEdge();

        graph.fireflySummaryUpdater.addEdgeRemoveToQueue(label);

        final FireflyVertex inVertex = this.graph.readVertex(this.inVid);
        final FireflyVertex outVertex = this.graph.readVertex(this.outVid);
        final FireflyIdFactory idFactory = this.graph.getIdFactory();
        if (inVertex != null) {
            inVertex.removeEdge(Direction.IN, idFactory.createCompositeEdgeId(this.id, this.outVid), this.label);
        }
        if (outVertex != null) {
            outVertex.removeEdge(Direction.OUT, idFactory.createCompositeEdgeId(this.id, this.inVid), this.label);
        }

        this.removed = true;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        // If there is only 1 key.
        if (propertyKeys.length == 1) {
            // And that key is null, return empty iterator.
            if (propertyKeys[0] == null) {
                return Collections.emptyIterator();
            }

            // Otherwise if there is only 1 key and it is not null, return the property if we have it, otherwise empty iterator.
            if (properties.containsKey(propertyKeys[0])) {
                return FireflyCloseableIteratorUtils.of(new PackedEdgeProperty<>(graph, this, propertyKeys[0],
                        (V) this.graph.getBaseGraph().convertValuetoTypeUsingHint(
                                properties.get(propertyKeys[0]), typeHints.get(propertyKeys[0]))));
            } else {
                return Collections.emptyIterator();
            }
        } else {
            // There are multiple keys.
            final List<Property<V>> propertyList = new ArrayList<>();
            for (final String key : properties.keySet()) {
                if (ElementHelper.keyExists(key, propertyKeys)) {
                    propertyList.add(new PackedEdgeProperty<>(graph, this, key,
                            (V) this.graph.getBaseGraph().convertValuetoTypeUsingHint(
                                    properties.get(key), typeHints.get(key))));
                }
            }
            return propertyList.iterator();
        }
    }

    public static <V> Property<V> writeProperty(final FireflyGraph graph, final FireflyEdge edge, final String propertyKey, final V value) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final Value edgeIdMapKey = Value.get(edge.id.getUserId());

        final Operation valueOp;
        final Operation typeHintOp;

        // Null value properties are not currently supported by Firefly and thus the correct behaviour is to remove
        // the property key if a null value is given.
        if (value == null) {
            valueOp = MapOperation.removeByKey(db.PROPERTIES, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(edgeIdMapKey));
            typeHintOp = MapOperation.removeByKey(db.TYPE_HINTS, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(edgeIdMapKey));
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            valueOp = MapOperation.put(policy, db.PROPERTIES, Value.get(propertyKey), Value.get(value),
                    CTX.mapKey(edgeIdMapKey));
            typeHintOp = MapOperation.put(policy, db.TYPE_HINTS, Value.get(propertyKey),
                    Value.get(getSupportedType(value)), CTX.mapKey(edgeIdMapKey));
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.operate(writePolicy, key, valueOp, typeHintOp);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case
                // the key is the Phat Edge key and thus the key still exists.
                throw new ElementNotFoundException(edge, ae);
            } else {
                throw ae;
            }
        }

        graph.fireflySummaryUpdater.addEdgePropertiesWriteToQueue(edge.label, Set.of(propertyKey));
        return new PackedEdgeProperty<>(graph, edge, propertyKey, value);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
