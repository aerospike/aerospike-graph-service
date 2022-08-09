package com.aerospike.firefly.io.impl.linked;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
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
    private AerospikeConnection db;

    // TODO: Possible performance enhancement. Cache the edge properties and keep them up to date here.

    /**
     * Constructor for LinkedEdge.
     *
     * @param fid       FireflyId to use.
     * @param label     Edge label.
     * @param graph     FireflyGraph to use.
     * @param outVertex Edge out vertex.
     * @param inVertex  Edge in vertex.
     */
    private LinkedEdge(final FireflyId fid,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId outVertex,
                       final FireflyId inVertex) {
        super(fid, label, outVertex, inVertex, graph);
        this.db = graph.getBaseGraph();
    }

    /**
     * Write edge including caching IN/OUT vertices and edge properties.
     *
     * @param graph      handle to Graph.
     * @param edgeId     Id of Edge to write.
     * @param label      label for Edge to write.
     * @param inVertex   in Vertex for new Edge.
     * @param outVertex  out Vertex for new Edge.
     * @param properties Edge properties.
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
        return new LinkedEdge(edgeId, label, graph, outVertex.id, inVertex.id);
    }

    /**
     * Read an Edge by the id.
     *
     * @param graph  Graph handle.
     * @param edgeId Edge id to read.
     * @return Edge.
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
                FireflyId.of(FireflyVertex.class, edgeRecord.record.getLong(Direction.OUT.name())),
                FireflyId.of(FireflyVertex.class, edgeRecord.record.getLong(Direction.IN.name())));
    }

    /**
     * Construct edge from record.
     *
     * @param graph     Graph handle.
     * @param keyRecord Record to construct from.
     * @return Edge.
     */
    public static LinkedEdge fromRecord(final FireflyGraph graph, final KeyRecord keyRecord) {
        LOG.trace("Constructing edge from record.");
        if (keyRecord == null) {
            return null;
        }
        final Record record = keyRecord.record;
        if (record == null) {
            return null;
        }
        return new LinkedEdge(
                FireflyId.loadFromAerospike(
                        graph.getBaseGraph(), FireflyVertex.class, FireflyRecord.fromRecord(graph.getBaseGraph(), keyRecord.key, record)),
                record.getString(AerospikeConnection.LABEL),
                graph,
                FireflyId.of(FireflyVertex.class, record.getLong(Direction.OUT.name())),
                FireflyId.of(FireflyVertex.class, record.getLong(Direction.IN.name())));
    }

    /**
     * Remove edge from Aerospike.
     */
    @Override
    public void removeEdge() {
        // Remove edge.
        LOG.debug("Removing edge {}.", this.id.value().toString());
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, id.toNumericId()));

        // Set flags to indicate vertex has been removed.
        this.removed = true;
    }
}
