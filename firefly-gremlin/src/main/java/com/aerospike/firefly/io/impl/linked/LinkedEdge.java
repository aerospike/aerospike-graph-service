package com.aerospike.firefly.io.impl.linked;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class LinkedEdge extends FireflyEdge {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedEdge.class);

    private List<Map.Entry<String, Object>> edgeProperties;
    private AerospikeConnection db;
    private boolean valid;

    /**
     * Constructor for LinkedEdge.
     *
     * @param fid            FireflyId to use.
     * @param label          Edge label.
     * @param graph          FireflyGraph to use.
     * @param edgeProperties Edge properties
     * @param outVertex      Edge out vertex.
     * @param inVertex       Edge in vertex.
     */
    private LinkedEdge(final FireflyId fid,
                       final String label,
                       final FireflyGraph graph,
                       final List<Map.Entry<String, Object>> edgeProperties,
                       final FireflyId outVertex,
                       final FireflyId inVertex) {
        super(fid, label, outVertex, inVertex, graph);
        this.valid = true;
        this.edgeProperties = edgeProperties;
        this.db = graph.getBaseGraph();
    }

    protected FireflyProperty writeProperty(final FireflyElement element, final String key, final Object value) {
        final AerospikeConnection db = graph.getBaseGraph();
        FireflyHelper.validatePropertyValue(value);
        db.writeTypeHintedValueToMap(
                db.getElementPropertySet(FireflyEdge.class),
                id,
                db.getElementPropertySet(FireflyEdge.class),
                key,
                value);
        return new LinkedProperty<>(graph, this, key, value);
    }

    protected Map<String, Property> readProperties() {
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db,
                db.getElementPropertySet(FireflyEdge.class), FireflyId.fromElement(this).toNumericId());
        if (fireflyRecord == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, Object> data = (Map<String, Object>) fireflyRecord.record.getMap(
                db.getElementPropertySet(FireflyEdge.class));
        if (data == null)
            return result;
        data.forEach((key1, value) -> {
            Property<Object> prop = readProperty(this, key1);
            result.put(key1, prop);
        });
        return result;
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * construct and return a Property from the value associated with k in the ELEMENT_PROPERTIES map
     *
     * @param element Element to read property from
     * @param key     property key
     * @param <V>     type
     * @return Property
     */
    private <V> Property<V> readProperty(final FireflyElement element, final String key) {
        return new LinkedProperty<>(graph, element, key,
                db.readTypeHintedValueFromMap(
                        db.getElementPropertySet(FireflyEdge.class),
                        FireflyId.fromElement(element).toNumericId(),
                        db.getElementPropertySet(FireflyEdge.class),
                        key));
    }

    /**
     * Write edge including caching IN/OUT vertices and edge properties.
     *
     * @param graph      handle to Graph
     * @param edgeId     Id of Edge to write
     * @param label      label for Edge to write
     * @param inVertex   in Vertex for new Edge
     * @param outVertex  out Vertex for new Edge
     * @param properties Edge properties
     */
    public static LinkedEdge writeEdge(final FireflyGraph graph,
                                       final FireflyId edgeId,
                                       final String label,
                                       final List<Map.Entry<String, Object>> properties,
                                       final FireflyVertex inVertex,
                                       final FireflyVertex outVertex) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId.value(), outVertex.id(), label, inVertex.id(), properties);

        final AerospikeConnection db = graph.getBaseGraph();
        final Map<String, Object> data = new HashMap<>();
        final Map<String, Object> typeHints = new HashMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value != null)
                typeHints.put(key, db.getSupportedType(value.getClass()));
            else
                typeHints.put(key, null);

            if (properties.stream().filter(p -> p.getKey().equals(key)).count() > 1) {
                typeHints.put(key, db.getSupportedType(List.class));
                if (data.containsKey(key)) {
                    ((List<Object>) (data.get(key))).add(value);
                } else {
                    List<Object> temp = new ArrayList<>();
                    temp.add(value);
                    data.put(key, temp);
                }
            } else {
                data.put(key, value);
            }

        });
        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(db.idToStorageType(inVertex.id())));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(db.idToStorageType(outVertex.id())));
        final Bin valueBin = new Bin(db.EDGE_AERO_SET, Value.get(data));
        final Bin typeHintBin = new Bin(db.TYPE_HINTS, Value.get(typeHints));
        FireflyRecord.writeElement(db, db.EDGE_AERO_SET, edgeId, labelBin, inVbin, outVBin, valueBin, typeHintBin);
        return new LinkedEdge(edgeId, label, graph, properties, outVertex.id, inVertex.id);
    }

    /**
     * Read an Edge by id
     *
     * @param graph  Graph handle
     * @param edgeId Edge id to read
     * @return Edge
     */
    public static LinkedEdge readEdge(final FireflyGraph graph, final FireflyId edgeId) {
        LOG.debug("Reading edge {}.", edgeId.value().toString());
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyRecord edgeRecord = FireflyRecord.read(db, db.EDGE_AERO_SET, edgeId.toNumericId());
        if (edgeRecord == null) {
            return null;
        }
        return new LinkedEdge(FireflyId.loadFromAerospike(db, FireflyEdge.class, edgeRecord),
                edgeRecord.record.getString(AerospikeConnection.LABEL),
                graph,
                new ArrayList<>(((Map<String, Object>) edgeRecord.record.getMap(db.EDGE_AERO_SET)).entrySet()),
                FireflyId.of(FireflyVertex.class, edgeRecord.record.getLong(Direction.OUT.name())),
                FireflyId.of(FireflyVertex.class, edgeRecord.record.getLong(Direction.IN.name())));
    }

    @Override
    public void removeEdge() {
        // Remove edge.
        LOG.debug("Removing Edge {}.", this.id.value().toString());
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, id.toNumericId()));

        // Set flags to indicate vertex has been removed.
        this.removed = true;
        this.valid = false;
    }
}
