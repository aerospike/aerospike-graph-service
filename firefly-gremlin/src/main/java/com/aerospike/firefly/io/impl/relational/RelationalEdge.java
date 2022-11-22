package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedEdge;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class RelationalEdge extends FireflyEdge {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalEdge.class);
    protected final AerospikeConnection db;

    // TODO: Possible performance enhancement. Cache the edge properties and keep them up to date here.

    /**
     * Constructor for RelationalEdge.
     *
     * @param fid       FireflyId to use.
     * @param label     Edge label.
     * @param graph     FireflyGraph to use.
     * @param outVertex Edge out vertex.
     * @param inVertex  Edge in vertex.
     */
    protected RelationalEdge(final FireflyId fid,
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
    public static RelationalEdge writeEdge(final FireflyGraph graph,
                                           final FireflyId edgeId,
                                           final String label,
                                           final List<Map.Entry<String, Object>> properties,
                                           final FireflyVertex inVertex,
                                           final FireflyVertex outVertex) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertex.id(), label, inVertex.id(), properties);

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
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(inVertex.id.getStorageId()));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(outVertex.id.getStorageId()));
        final Bin valueBin = new Bin(db.EDGE_AERO_SET, Value.get(data));
        final Bin typeHintBin = new Bin(db.TYPE_HINTS, Value.get(typeHints));

        // First instance of this edge, generation -1.
        FireflyRecord.writeElement(db, db.EDGE_AERO_SET, edgeId, -1, labelBin, inVbin, outVBin, valueBin, typeHintBin);
        return RelationalEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id);
    }

    /**
     * Read an Edge by the id.
     *
     * @param graph  Graph handle.
     * @param edgeId Edge id to read.
     * @return Edge.
     */
    public static RelationalEdge readEdge(final FireflyGraph graph, final FireflyId edgeId) {
        LOG.debug("Reading edge {}.", edgeId.toString());
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyRecord edgeRecord = FireflyRecord.read(db, db.EDGE_AERO_SET, edgeId);
        if (edgeRecord == null) {
            return null;
        }
        return RelationalEdgeFactory.create(FireflyIdFactory.createFromRecord(db, edgeRecord),
                edgeRecord.record.getString(AerospikeConnection.LABEL),
                graph,
                FireflyIdFactory.createId(edgeRecord.record.getLong(Direction.OUT.name())),
                FireflyIdFactory.createId(edgeRecord.record.getLong(Direction.IN.name())));
    }

    /**
     * Read an Edge by the id.
     *
     * @param graph   Graph handle.
     * @param edgeIds Edge ids to read.
     * @return Edge.
     */
    public static List<FireflyEdge> readEdges(final FireflyGraph graph, final List<FireflyId> edgeIds) {
        LOG.debug("Reading edges {}.", edgeIds.toString());
        final AerospikeConnection db = graph.getBaseGraph();

        final List<FireflyRecord> edgeRecord = FireflyRecord.batchRead(db, db.EDGE_AERO_SET, edgeIds);
        if (edgeRecord == null) {
            return new ArrayList<>();
        }
        return edgeRecord.stream().map(record -> RelationalEdgeFactory.create(
                FireflyIdFactory.createFromRecord(db, record),
                        record.record.getString(AerospikeConnection.LABEL),
                        graph,
                        FireflyIdFactory.createId(record.record.getLong(Direction.OUT.name())),
                        FireflyIdFactory.createId(record.record.getLong(Direction.IN.name())))).
                collect(Collectors.toList());
    }

    /**
     * Construct edge from record.
     *
     * @param graph     Graph handle.
     * @param keyRecord Record to construct from.
     * @return Edge.
     */
    public static RelationalEdge fromRecord(final FireflyGraph graph, final KeyRecord keyRecord) {
        LOG.trace("Constructing edge from record.");
        if (keyRecord == null) {
            return null;
        }
        final Record record = keyRecord.record;
        if (record == null) {
            return null;
        }
        return RelationalEdgeFactory.create(
                FireflyIdFactory.createFromRecord(
                        graph.getBaseGraph(), FireflyRecord.fromRecord(graph.getBaseGraph(), keyRecord.key, record)),
                record.getString(AerospikeConnection.LABEL),
                graph,
                FireflyIdFactory.createId(record.getLong(Direction.OUT.name())),
                FireflyIdFactory.createId(record.getLong(Direction.IN.name())));
    }

    /**
     * Remove edge from Aerospike.
     */
    @Override
    public void removeEdge() {
        // Remove edge.
        LOG.debug("Removing edge {}.", this.id);

        db.delete(FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, id));
    }

    private static class RelationalEdgeFactory {
        private static RelationalEdge create(final FireflyId fid, final String label, final FireflyGraph graph,
                                             final FireflyId outVertex, final FireflyId inVertex) {
            if (StarPackedGraph.isStarPackedGraph(graph)) {
                return new StarPackedEdge(fid, label, graph, outVertex, inVertex);
            } else {
                return new RelationalEdge(fid, label, graph, outVertex, inVertex);
            }
        }
    }
}
