package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
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
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
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
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                             final FireflyId inVertex,
                             final Map<String, Object> properties,
                             final Map<String, Long> typeHints) {
        super(fid, label, graph, inVertex, outVertex, properties, typeHints);
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
        final Map<String, Object> data = new TreeMap<>();
        final Map<String, Long> typeHints = new TreeMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value == null) {
                data.remove(key);
                typeHints.remove(key);
            } else {
                typeHints.put(key, getSupportedType(value.getClass()));
                data.put(key, value);
            }
        });

        // CREATE_ONLY as writing an edge will always have a newly-generated unique ID.
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY);
        final Operation writeLabel = MapOperation.put(mapPolicy, AerospikeConnection.LABEL,
                Value.get(edgeId.getUserId()), Value.get(label));
        final Operation writeInV = MapOperation.put(mapPolicy, Direction.IN.name(),
                Value.get(edgeId.getUserId()), Value.get(inVertex.id.getKeyHashBase64()));
        final Operation writeOutV = MapOperation.put(mapPolicy, Direction.OUT.name(),
                Value.get(edgeId.getUserId()), Value.get(outVertex.id.getKeyHashBase64()));
        final Operation writeProperties = MapOperation.put(mapPolicy, db.PROPERTIES,
                Value.get(edgeId.getUserId()), Value.get(data, MapOrder.KEY_ORDERED));
        final Operation writeTypeHints = MapOperation.put(mapPolicy, db.TYPE_HINTS,
                Value.get(edgeId.getUserId()), Value.get(typeHints, MapOrder.KEY_ORDERED));

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.maxRetries = db.AEROSPIKE_WRITE_MAX_RETRY;
        final Key key = getKey(db, db.EDGE_AERO_SET, edgeId);
        db.operate(writePolicy, key, writeLabel, writeInV, writeOutV, writeProperties, writeTypeHints);
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
     * Construct edge from record.
     *
     * @param graph     Graph handle.
     * @param keyRecord Record to construct from.
     * @return Edge.
     */
    public static RelationalEdge fromRecord(final FireflyGraph graph, final KeyRecord keyRecord, final FireflyId edgeId) {
        LOG.trace("Constructing edge from record.");
        if (keyRecord == null) {
            return null;
        }
        final FireflyRecord fireflyRecord = FireflyRecord.fromRecord(graph.getBaseGraph(), keyRecord);
        return RelationalEdgeFactory.create(edgeId, fireflyRecord, graph);
    }

    /**
     * Construct edges from phat edge record.
     *
     * @param graph     Graph handle.
     * @param keyRecord Record to construct from.
     * @return Iterator of FireflyEdge.
     */
    public static Iterator<FireflyEdge> allFromRecord(final FireflyGraph graph, final KeyRecord keyRecord) {
        LOG.trace("Constructing edges from phat edge record.");
        if (keyRecord == null || keyRecord.record == null) {
            return Collections.emptyIterator();
        }

        final List<FireflyEdge> edges = new ArrayList<>();
        final Map<Long, String> labelMap = (TreeMap<Long, String>) keyRecord.record.getMap(AerospikeConnection.LABEL);
        for (final Long edgeId : labelMap.keySet()) {
            final FireflyId fireflyEdgeId = new FireflyPhatEdgeId(edgeId, graph.getBaseGraph().PHAT_EDGE_SIZE,
                    graph.getBaseGraph().EDGE_AERO_SET);
            final FireflyEdge edge = RelationalEdgeFactory.create(fireflyEdgeId,
                    FireflyRecord.fromRecord(graph.getBaseGraph(), keyRecord), graph);
            edges.add(edge);
        }

        return edges.iterator();
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

        final Operation removeLabel = MapOperation.removeByKey(AerospikeConnection.LABEL, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeIn = MapOperation.removeByKey(Direction.IN.name(), Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeOut = MapOperation.removeByKey(Direction.OUT.name(), Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeProperties = MapOperation.removeByKey(db.PROPERTIES, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeTypeHints = MapOperation.removeByKey(db.TYPE_HINTS, Value.get(edgeId.getUserId()), MapReturnType.NONE);

        // Logic for deleting the entire phat edge record if it no longer contains individual edges.
        final Expression removeEmptyPhatEdgeExp = Exp.build(
                // If the size of the label map, which implicitly is the amount of edges in the phat edge, is 0, write
                // null. Otherwise, fail.
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(AerospikeConnection.LABEL)), Exp.val(0)),
                        Exp.nil(),
                        Exp.unknown()
                )
        );
        // If all bins in a record contain null, the record is implicitly deleted.
        final int deletePhatEdgeWriteFlags = ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.ALLOW_DELETE;
        final Operation removeInBin = ExpOperation.write(Direction.IN.name(), removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeOutBin = ExpOperation.write(Direction.OUT.name(), removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removePropertiesBin = ExpOperation.write(db.PROPERTIES, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeTypeHintsBin = ExpOperation.write(db.TYPE_HINTS, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        
        // This operation must be last since the expression checks the map in the label bin.
        final Operation removeLabelBin = ExpOperation.write(AerospikeConnection.LABEL, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);

        try {
            db.operate(null, key, removeLabel, removeIn, removeOut, removeProperties, removeTypeHints,
                    removeInBin, removeOutBin, removePropertiesBin, removeTypeHintsBin, removeLabelBin);
        } catch (final ElementNotFoundException e) {
            // This tends to occur when deleting multiple vertices in a single traversal where the Edge lives in between
            // the to-be-deleted vertices.
            LOG.info("Ignoring exception when deleting Edge with id " + edgeId.getUserId() + " since it was not found.");
        }
    }

    public <V> Property<V> writeProperty(final String propertyKey, final V value) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, this.id);
        final Value edgeIdMapKey = Value.get(this.id.getUserId());

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
                    Value.get(getSupportedType(value.getClass())), CTX.mapKey(edgeIdMapKey));
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.operate(writePolicy, key, valueOp, typeHintOp);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case
                // the key is the Phat Edge key and thus the key still exists.
                throw new ElementNotFoundException(this, ae);
            } else {
                throw ae;
            }
        }
        graph.fireflySummaryUpdater.addEdgePropertiesWriteToQueue(label, Set.of(propertyKey));
        return new RelationalProperty<>(graph, this, propertyKey, value);
    }

    public void removeProperty(final String propertyKey) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, this.id);
        final Value edgeIdMapKey = Value.get(this.id.getUserId());

        final Operation removeProperty = MapOperation.removeByKey(db.PROPERTIES, Value.get(propertyKey),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey));
        final Operation removeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS, Value.get(propertyKey),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey));

        try {
            this.properties.remove(propertyKey);
            db.operate(null, key, removeProperty, removeTypeHint);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case the key is
                // the Phat Edge key and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed property {}", this, ae);
            } else {
                throw ae;
            }
        }
    }

    private static class RelationalEdgeFactory {
        private static RelationalEdge create(final FireflyId fid, final String label, final FireflyGraph graph,
                                             final FireflyId outVertex, final FireflyId inVertex,
                                             final Map<String, Object> properties, final Map<String, Long> typeHints) {
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
            final long edgeIdMapKey = (long) edgeId.getUserId();
            final Map<Long, String> labels = (Map<Long, String>) record.getMap(AerospikeConnection.LABEL);
            // Implicitly assume that if the key is found for label, which is required, then the key exists for the
            // other phat edge maps, since they are all written in the same operate.
            if (!labels.containsKey(edgeIdMapKey)) {
                return null;
            }
            final String label = labels.get(edgeIdMapKey);
            final FireflyId outVertex = FireflyIdPoly.fromBase64Hash((String) record.getMap(Direction.OUT.name()).get(edgeIdMapKey), db.VERTEX_AERO_SET);
            final FireflyId inVertex = FireflyIdPoly.fromBase64Hash((String) record.getMap(Direction.IN.name()).get(edgeIdMapKey), db.VERTEX_AERO_SET);
            final Map<String, Object> properties = (Map<String, Object>) record.getMap(db.PROPERTIES).get(edgeIdMapKey);
            final Map<String, Long> typeHints = (Map<String, Long>) record.getMap(db.TYPE_HINTS).get(edgeIdMapKey);

            return RelationalEdgeFactory.create(edgeId, label, graph, outVertex, inVertex, properties, typeHints);
        }
    }
}
