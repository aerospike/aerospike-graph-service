package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedEdge;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.io.utils.ElementNotFoundException;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class RelationalEdge extends FireflyEdge {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalEdge.class);
    protected final AerospikeConnection db;

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
                             final FireflyId inVertex,
                             final Map<String, Object> properties,
                             final Map<String, Object> typeHints) {
        super(fid, label, graph, inVertex, outVertex, properties, typeHints);
        this.db = graph.getBaseGraph();
    }

    @Override
    public Object id() {
        return Base64.getEncoder().encodeToString(((ByteBuffer) id.getUserId()).array());
    }

    /**
     * Remove property from edge property cache.
     *
     * @param key Key to remove.
     */
    public void removePropertyFromCache(final String key) {
        properties.remove(key);
        typeHints.remove(key);
    }

    /**
     * Write edge including caching IN/OUT vertices and edge properties.
     *
     * @param graph                 handle to Graph.
     * @param edgeId                Id of Edge to write.
     * @param label                 label for Edge to write.
     * @param inVertex              in Vertex for new Edge.
     * @param outVertex             out Vertex for new Edge.
     * @param inVertexCacheWrite    was the edge written to the edge cache of the in vertex.
     * @param outVertexCacheWrite   was the edge written to the edge cache of the out vertex.
     * @param properties Edge properties.
     */
    public static RelationalEdge writeEdge(final FireflyGraph graph,
                                           final FireflyId edgeId,
                                           final String label,
                                           final List<Map.Entry<String, Object>> properties,
                                           final FireflyVertex inVertex,
                                           final FireflyVertex outVertex,
                                           final boolean inVertexCacheWrite,
                                           final boolean outVertexCacheWrite) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertex.id(), label, inVertex.id(), properties);

        final AerospikeConnection db = graph.getBaseGraph();
        final Map<String, Object> data = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value == null) {
                data.remove(key);
                typeHints.remove(key);
            } else {
                typeHints.put(key, getSupportedType(value));
                data.put(key, value);
            }
        });

        final List<Operation> operations = new ArrayList<>();
        // CREATE_ONLY as writing an edge will always have a newly-generated unique ID.
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY);
        final Operation writeLabel = MapOperation.put(mapPolicy,db.LABEL_BIN,
                Value.get(edgeId.getUserId()), Value.get(label));
        operations.add(writeLabel);

        final Operation writeInV = MapOperation.put(mapPolicy, Direction.IN.name(),
                Value.get(edgeId.getUserId()), Value.get(inVertex.id.getKeyHashBase64()));
        operations.add(writeInV);
        final Operation writeOutV = MapOperation.put(mapPolicy, Direction.OUT.name(),
                Value.get(edgeId.getUserId()), Value.get(outVertex.id.getKeyHashBase64()));
        operations.add(writeOutV);

        // Write to supernodes bin if vertex cache overflowed.
        if (!inVertexCacheWrite) {
            final Operation writeInVSupernode = MapOperation.put(mapPolicy, db.SUPERNODES_IN_BIN,
                    Value.get(edgeId.getUserId()), Value.get(inVertex.id.getKeyHashBase64()));
            operations.add(writeInVSupernode);
        }
        if (!outVertexCacheWrite) {
            final Operation writeOutVSupernode = MapOperation.put(mapPolicy, db.SUPERNODES_OUT_BIN,
                    Value.get(edgeId.getUserId()), Value.get(outVertex.id.getKeyHashBase64()));
            operations.add(writeOutVSupernode);
        }

        final Operation writeProperties = MapOperation.put(mapPolicy, db.PROPERTIES_BIN,
                Value.get(edgeId.getUserId()), Value.get(data, MapOrder.KEY_ORDERED));
        operations.add(writeProperties);
        final Operation writeTypeHints = MapOperation.put(mapPolicy, db.TYPE_HINTS_BIN,
                Value.get(edgeId.getUserId()), Value.get(typeHints, MapOrder.KEY_ORDERED));
        operations.add(writeTypeHints);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.maxRetries = db.AEROSPIKE_WRITE_MAX_RETRY;
        final Key key = getKey(db, db.EDGE_AERO_SET, edgeId);
        db.operate(writePolicy, key, operations.toArray(new Operation[0]));
        graph.fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
        return RelationalEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id, data, typeHints);
    }

    /**
     * Read Edges with the provided list of IDs.
     *
     * @param graph   Graph handle.
     * @param edgeIds Edge ids to read.
     * @return Edge.
     */
    public static List<FireflyEdge> readEdges(final FireflyGraph graph, final List<FireflyId> edgeIds) {
        LOG.debug("Reading edges {}.", edgeIds.toString());
        final AerospikeConnection db = graph.getBaseGraph();

        final List<FireflyEdge> edges = new ArrayList<>();
        final Map<FireflyId, FireflyRecord> edgeRecords = FireflyRecord.batchReadPhatEdges(db, edgeIds);
        if (edgeRecords.isEmpty()) {
            return edges;
        }

        for (final FireflyId edgeId : edgeIds) {
            final FireflyRecord edgeRecord = edgeRecords.get(edgeId);
            if (edgeRecord != null) {
                final RelationalEdge edge = RelationalEdgeFactory.create(edgeId, edgeRecord, graph);
                if (edge != null) {
                    edges.add(edge);
                }
            }
        }

        return edges;
    }

    /**
     * Remove edge from Aerospike.
     */
    @Override
    public void removeEdge() {
        removeEdgeById(this.graph, this.id);
    }

    /**
     * Function to remove edge record via id without reading the edge back.
     * NOTE: This function does not remove the edge from adjacent vertices. This must be done separately.
     *
     * @param graph  Graph handle.
     * @param edgeId Id of edge to remove.
     */
    public static void removeEdgeById(final FireflyGraph graph, final FireflyId edgeId) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, edgeId);

        final Operation removeLabel = MapOperation.removeByKey(db.LABEL_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeIn = MapOperation.removeByKey(Direction.IN.name(), Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeOut = MapOperation.removeByKey(Direction.OUT.name(), Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeProperties = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeTypeHints = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeSupernodesIn = MapOperation.removeByKey(db.SUPERNODES_IN_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeSupernodesOut = MapOperation.removeByKey(db.SUPERNODES_OUT_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);

        // Logic for deleting the entire phat edge record if it no longer contains individual edges.
        final Expression removeEmptyPhatEdgeExp = Exp.build(
                // If the size of the label map, which implicitly is the amount of edges in the phat edge, is 0, write
                // null. Otherwise, fail.
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(db.LABEL_BIN)), Exp.val(0)),
                        Exp.nil(),
                        Exp.unknown()
                )
        );
        // If all bins in a record contain null, the record is implicitly deleted.
        final int deletePhatEdgeWriteFlags = ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.ALLOW_DELETE;
        final Operation removeInBin = ExpOperation.write(Direction.IN.name(), removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeOutBin = ExpOperation.write(Direction.OUT.name(), removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removePropertiesBin = ExpOperation.write(db.PROPERTIES_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeTypeHintsBin = ExpOperation.write(db.TYPE_HINTS_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeSupernodesInBin = ExpOperation.write(db.SUPERNODES_IN_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeSupernodesOutBin = ExpOperation.write(db.SUPERNODES_OUT_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);

        // This operation must be last since the expression checks the map in the label bin.
        final Operation removeLabelBin = ExpOperation.write(db.LABEL_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);

        try {
            db.operate(null, key, removeLabel, removeIn, removeOut, removeProperties, removeTypeHints,
                    removeSupernodesIn, removeSupernodesOut, removeInBin, removeOutBin, removePropertiesBin,
                    removeTypeHintsBin, removeSupernodesInBin, removeSupernodesOutBin, removeLabelBin);
        } catch (final ElementNotFoundException e) {
            // This tends to occur when deleting multiple vertices in a single traversal where the Edge lives in between
            // the to-be-deleted vertices.
            LOG.info("Ignoring exception when deleting Edge with id " + edgeId.getUserId() + " since it was not found.");
        }
    }

    private static class RelationalEdgeFactory {
        private static RelationalEdge create(final FireflyId fid, final String label, final FireflyGraph graph,
                                             final FireflyId outVertex, final FireflyId inVertex,
                                             final Map<String, Object> properties, final Map<String, Object> typeHints) {
            if (StarPackedGraph.isStarPackedGraph(graph)) {
                return new StarPackedEdge(fid, label, graph, outVertex, inVertex, properties, typeHints);
            } else {
                return new RelationalEdge(fid, label, graph, outVertex, inVertex, properties, typeHints);
            }
        }

        private static RelationalEdge create(final FireflyId edgeId, final FireflyRecord fireflyRecord,
                                             final FireflyGraph graph) {
            if (fireflyRecord == null || fireflyRecord.record() == null) {
                return null;
            }
            final AerospikeConnection db = graph.getBaseGraph();
            final Record record = fireflyRecord.record();
            final ByteBuffer edgeIdMapKey = (ByteBuffer) edgeId.getUserId();
            final Map<ByteBuffer, String> labels = (Map<ByteBuffer, String>) record.getMap(db.LABEL_BIN);
            // Implicitly assume that if the key is found for label, which is required, then the key exists for the
            // other phat edge maps, since they are all written in the same operate.
            if (!labels.containsKey(edgeIdMapKey)) {
                return null;
            }
            final String label = labels.get(edgeIdMapKey);

            // If adjacency indexes are enabled the OUT and IN Vertex IDs might be stored in the adjacency index bins
            // instead of the regular ones.
            Map<?, ?> outVMap = record.getMap(Direction.OUT.name());
            if ((outVMap == null || !outVMap.containsKey(edgeIdMapKey))) {
                if (db.ADJACENCY_INDEX_ENABLED_FLAG) {
                    outVMap = record.getMap(db.SUPERNODES_OUT_BIN);
                }
                if ((outVMap == null || !outVMap.containsKey(edgeIdMapKey))) {
                    LOG.error("Could not find OUT Vertex ID for Edge ID {}.", edgeId.getUserId());
                    return null;
                }
            }
            final FireflyId outVertex = FireflyIdPoly.fromBase64Hash((String) outVMap.get(edgeIdMapKey), db.VERTEX_AERO_SET);

            Map<?, ?> inVMap = record.getMap(Direction.IN.name());
            if ((inVMap == null || !inVMap.containsKey(edgeIdMapKey))) {
                if (db.ADJACENCY_INDEX_ENABLED_FLAG) {
                    inVMap = record.getMap(db.SUPERNODES_IN_BIN);
                }
                if ((inVMap == null || !inVMap.containsKey(edgeIdMapKey))) {
                    LOG.error("Could not find IN Vertex ID for Edge ID {}.", edgeId.getUserId());
                    return null;
                }
            }
            final FireflyId inVertex = FireflyIdPoly.fromBase64Hash((String) inVMap.get(edgeIdMapKey), db.VERTEX_AERO_SET);

            final Map<String, Object> properties = (Map<String, Object>) record.getMap(db.PROPERTIES_BIN).get(edgeIdMapKey);
            final Map<String, Object> typeHints = (Map<String, Object>) record.getMap(db.TYPE_HINTS_BIN).get(edgeIdMapKey);

            return RelationalEdgeFactory.create(edgeId, label, graph, outVertex, inVertex, properties, typeHints);
        }
    }
}
