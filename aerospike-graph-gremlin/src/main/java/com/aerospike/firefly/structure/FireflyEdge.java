package com.aerospike.firefly.structure;

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
import com.aerospike.client.command.Command;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException;
import com.aerospike.firefly.runtime.exceptions.ElementNotFoundException;
import com.aerospike.firefly.runtime.exceptions.RecordTooBigException;
import com.aerospike.firefly.runtime.exceptions.TtlNotEnabledException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.fromAddingEdge;
import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.fromAddingProperty;
import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.getUserIdString;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyEdge extends FireflyElement implements Edge {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyEdge.class);
    // Individual edges' data are stored in a List within the phat edge.
    // These are the indexes in the List for where each value is stored.
    public static final int LABEL_POSITION = 0;
    public static final int IN_V_POSITION = 1;
    public static final int OUT_V_POSITION = 2;
    public static final int PROPERTIES_POSITION = 3;
    public static final int TYPE_HINTS_POSITION = 4;
    public static final int EDGE_DATA_SIZE = 5;
    public static final String EDGE_SUPERNODE_LABEL_KEY = T.label.getAccessor();
    public static final String EDGE_SUPERNODE_OUT_KEY = "~OUT";
    public static final String EDGE_SUPERNODE_IN_KEY = "~IN";

    protected final AerospikeConnection db;
    public boolean removed;
    protected final FireflyGraph graph;
    protected final FireflyId inVid;
    protected final FireflyId outVid;
    protected final Map<String, Object> properties;
    protected final Map<String, Object> typeHints;
    private final boolean isInSupernode;
    private final boolean isOutSupernode;
    private final int generation;

    public FireflyEdge(final FireflyPhatEdgeId id,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId outVid,
                       final FireflyId inVid,
                       final Map<String, Object> properties,
                       final Map<String, Object> typeHints,
                       final boolean isOutSupernode,
                       final boolean isInSupernode,
                       final int generation) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
        this.properties = properties;
        this.typeHints = typeHints;
        this.db = graph.getBaseGraph();
        this.isOutSupernode = isOutSupernode;
        this.isInSupernode = isInSupernode;
        this.generation = generation;
    }

    /**
     * Write edge to record. This does not write to edge caches on attached vertices.
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
    public static FireflyEdge writeEdge(final FireflyGraph graph,
                                        final FireflyId edgeId,
                                        final String label,
                                        final List<Map.Entry<String, Object>> properties,
                                        final FireflyVertex inVertex,
                                        final FireflyVertex outVertex,
                                        final boolean inVertexCacheWrite,
                                        final boolean outVertexCacheWrite) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertex.id(), label, inVertex.id(), properties);

        final AerospikeConnection db = graph.getBaseGraph();
        final Map<String, Object> propertyMap = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        properties.forEach(property -> {
            final String key = property.getKey();
            final Object value = property.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value == null) {
                propertyMap.remove(key);
                typeHints.remove(key);
            } else {
                final Object typeHint = getTypeHintOf(value);
                if (typeHint != null) {
                    typeHints.put(key, typeHint);
                }
                propertyMap.put(key, value);
            }
        });

        final List<Value> edgeData = new ArrayList<>(EDGE_DATA_SIZE);

        final List<Operation> operations = new ArrayList<>();
        // CREATE_ONLY as writing an edge will always have a newly-generated unique ID.
        final MapPolicy edgeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED,
                MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL | MapWriteFlags.PARTIAL);

        // Add label to Edge data.
        edgeData.add(LABEL_POSITION, Value.get(label));
        // Add IN and OUT to Edge data.
        edgeData.add(IN_V_POSITION, Value.get(inVertex.id.getKeyHashString()));
        edgeData.add(OUT_V_POSITION, Value.get(outVertex.id.getKeyHashString()));

        // Write to supernodes bin if vertex cache overflowed.
        if (!inVertexCacheWrite) {
            final Operation writeInVSupernode = MapOperation.put(edgeMapPolicy, db.SUPERNODES_IN_BIN,
                    Value.get(edgeId.getUserId()), Value.get(inVertex.id.getKeyHashString()));
            operations.add(writeInVSupernode);
        }
        if (!outVertexCacheWrite) {
            final Operation writeOutVSupernode = MapOperation.put(edgeMapPolicy, db.SUPERNODES_OUT_BIN,
                    Value.get(edgeId.getUserId()), Value.get(outVertex.id.getKeyHashString()));
            operations.add(writeOutVSupernode);
        }

        // Handle TTL.
        long ttlValueLong = 0;
        if (propertyMap.containsKey(TTL_PROPERTY_KEY)) {
            if (!db.TTL_ENABLED_FLAG) {
                throw new TtlNotEnabledException();
            }
            final Object ttlValue = propertyMap.remove(TTL_PROPERTY_KEY);
            typeHints.remove(TTL_PROPERTY_KEY);
            if (Number.class.isAssignableFrom(ttlValue.getClass())) {
                ttlValueLong = ((Number) ttlValue).longValue();
                final long expirationTime = System.currentTimeMillis() + (ttlValueLong * 1000);
                final Operation writeTtl = MapOperation.put(edgeMapPolicy, db.TTL_BIN, Value.get(edgeId.getUserId()),
                        Value.get(expirationTime));
                operations.add(writeTtl);
            } else {
                throw new IllegalArgumentException(
                        String.format("Property value [%s] for key %s is of type %s and must be numeric", ttlValue,
                                TTL_PROPERTY_KEY, ttlValue.getClass()));
            }
        }

        // Write to filterable supernode bin if necessary.
        operations.addAll(createFilterableSupernodeOperations(graph, (FireflyPhatEdgeId) edgeId, !outVertexCacheWrite,
                !inVertexCacheWrite, outVertex.id, inVertex.id, label, propertyMap));

        // Add properties and type hints to Edge data.
        edgeData.add(PROPERTIES_POSITION, Value.get(propertyMap));
        edgeData.add(TYPE_HINTS_POSITION, Value.get(typeHints));

        // Create Operation for writing Edge data.
        final Operation createIndividualEdgeMap = MapOperation.put(edgeMapPolicy, db.EDGE_DATA_BIN,
                Value.get(edgeId.getUserId()), Value.get(edgeData));
        operations.add(createIndividualEdgeMap);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        final Key key = getKey(db, db.EDGE_AERO_SET, edgeId);
        try {
            final Record record = db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));
            graph.fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
            final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id,
                    propertyMap, typeHints, !outVertexCacheWrite, !inVertexCacheWrite, record.generation);
            return edge;
        } catch (final RecordTooBigException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingEdge((AerospikeException) e.getCause(), db, key, edgeId);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    public static List<Operation> createFilterableSupernodeOperations(final FireflyGraph graph, final FireflyPhatEdgeId edgeId,
                                                                      final boolean isOutSupernode, final boolean isInSupernode,
                                                                      final FireflyId outVId, final FireflyId inVId,
                                                                      final String label, final Map<String, Object> propertyMap) {
        // This method is concurrent traversal safe for removed edges since it's only invoked on creation of a new edge.
        if (!graph.getBaseGraph().isSupernodePushdownEnabled || (!isOutSupernode && !isInSupernode)) {
            // No supernodes so we don't have to do anything
            return Collections.emptyList();
        }

        final String binName = graph.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN;
        final Value edgeUniqueId = Value.get(edgeId.getUniqueId());
        final Value outVIdValue = Value.get(outVId.getKeyHashString());
        final Value inVIdValue = Value.get(inVId.getKeyHashString());

        final List<Operation> operations = new ArrayList<>();
        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        // Adjacent Vertex ID and Label
        if (isOutSupernode) {
            final Operation labelOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(label),
                    CTX.mapKeyCreate(outVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_LABEL_KEY), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
            if (graph.getBaseGraph().isMergeEdgeDataModelEnabled) {
                final Operation adjacentVOperation = MapOperation.put(policy, binName, edgeUniqueId, inVIdValue,
                        CTX.mapKeyCreate(outVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_IN_KEY), MapOrder.KEY_ORDERED));
                operations.add(adjacentVOperation);
            }
        }
        if (isInSupernode) {
            final Operation labelOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(label),
                    CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_LABEL_KEY), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
            if (graph.getBaseGraph().isMergeEdgeDataModelEnabled) {
                final Operation adjacentVOperation = MapOperation.put(policy, binName, edgeUniqueId, outVIdValue,
                        CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_OUT_KEY), MapOrder.KEY_ORDERED));
                operations.add(adjacentVOperation);
            }
        }
        // Properties
        for (final Map.Entry<String, Object> property : propertyMap.entrySet()) {
            appendFilterableSupernodePropertyOperation(graph, edgeId, isOutSupernode, isInSupernode, outVId, inVId,
                    property.getKey(), property.getValue(), operations);
        }
        return operations;
    }

    public static void appendFilterableSupernodePropertyOperation(final FireflyGraph graph, final FireflyPhatEdgeId edgeId,
                                                                  final boolean isOutSupernode, final boolean isInSupernode,
                                                                  final FireflyId outVId, final FireflyId inVId,
                                                                  final String propertyKey, final Object propertyValue,
                                                                  final List<Operation> operations) {
        // This method is concurrent traversal safe for removed edges since this should only be invoked with conjunction
        // of adding a property normally, which have operations that fail if the edge was already removed.
        if (!graph.getBaseGraph().isSupernodePushdownEnabled || (!isOutSupernode && !isInSupernode)) {
            // No supernodes so we don't have to do anything
            return;
        }

        final String binName = graph.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN;
        final Value edgeUniqueId = Value.get(edgeId.getUniqueId());
        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);

        if (isPropertyValuePushdownable(propertyValue)) {
            if (isOutSupernode) {
                final Value outVIdValue = Value.get(outVId.getKeyHashString());
                final Operation propertyOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(propertyValue),
                        CTX.mapKey(outVIdValue), CTX.mapKeyCreate(Value.get(propertyKey), MapOrder.KEY_ORDERED));
                operations.add(propertyOperation);
            }
            if (isInSupernode) {
                final Value inVIdValue = Value.get(inVId.getKeyHashString());
                final Operation propertyOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(propertyValue),
                        CTX.mapKey(inVIdValue), CTX.mapKeyCreate(Value.get(propertyKey), MapOrder.KEY_ORDERED));
                operations.add(propertyOperation);
            }
        }
    }

    public static void appendRemoveFilterableSupernodePropertyOperation(final FireflyEdge fireflyEdge,
                                                                        final String propertyKey,
                                                                        final List<Operation> operations) {
        final boolean isOutSupernode = fireflyEdge.isOutSupernode;
        final boolean isInSupernode = fireflyEdge.isInSupernode;
        final FireflyGraph graph = fireflyEdge.graph;
        final FireflyPhatEdgeId edgeId = (FireflyPhatEdgeId) fireflyEdge.id;

        if (!graph.getBaseGraph().isSupernodePushdownEnabled || (!isOutSupernode && !isInSupernode)) {
            // No supernodes so we don't have to do anything
            return;
        }

        final String binName = graph.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN;
        final Exp edgeUniqueId = Exp.val(edgeId.getUniqueId());
        final Value propertyKeyValue = Value.get(propertyKey);
        final Exp propertyKeyExp = Exp.val(propertyKey);
        final int writeFlags = ExpWriteFlags.EVAL_NO_FAIL;

        if (isOutSupernode) {
            // Remove Edge ID : Property Value
            final Value outVIdValue = Value.get(fireflyEdge.outVid.getKeyHashString());
            final Expression removeEdgeIdToPropertyExp = Exp.build(
                    MapExp.removeByKey(edgeUniqueId, Exp.mapBin(binName),
                            CTX.mapKey(outVIdValue), CTX.mapKey(propertyKeyValue)));
            final Operation removeEdgeIdToProperty = ExpOperation.write(binName, removeEdgeIdToPropertyExp, writeFlags);
            operations.add(removeEdgeIdToProperty);
            // Remove Property Key if empty
            final Expression removePropertyKeyExp = Exp.build(
                    Exp.cond(
                            Exp.eq(MapExp.size(Exp.mapBin(binName), CTX.mapKey(outVIdValue), CTX.mapKey(propertyKeyValue)), Exp.val(0)),
                            MapExp.removeByKey(propertyKeyExp, Exp.mapBin(binName), CTX.mapKey(outVIdValue)),
                            Exp.unknown()
                    )
            );
            final Operation removePropertyKey = ExpOperation.write(binName, removePropertyKeyExp, writeFlags);
            operations.add(removePropertyKey);
        }
        if (isInSupernode) {
            // Remove Edge ID : Property Value
            final Value inVIdValue = Value.get(fireflyEdge.inVid.getKeyHashString());
            final Expression removeEdgeIdToPropertyExp = Exp.build(
                    MapExp.removeByKey(edgeUniqueId, Exp.mapBin(binName),
                            CTX.mapKey(inVIdValue), CTX.mapKey(propertyKeyValue)));
            final Operation removeEdgeIdToProperty = ExpOperation.write(binName, removeEdgeIdToPropertyExp, writeFlags);
            operations.add(removeEdgeIdToProperty);
            // Remove Property Key if empty
            final Expression removePropertyKeyExp = Exp.build(
                    Exp.cond(
                            Exp.eq(MapExp.size(Exp.mapBin(binName), CTX.mapKey(inVIdValue), CTX.mapKey(propertyKeyValue)), Exp.val(0)),
                            MapExp.removeByKey(propertyKeyExp, Exp.mapBin(binName), CTX.mapKey(inVIdValue)),
                            Exp.unknown()
                    )
            );
            final Operation removePropertyKey = ExpOperation.write(binName, removePropertyKeyExp, writeFlags);
            operations.add(removePropertyKey);
        }
    }

    public static boolean isPropertyValuePushdownable(final Object propertyValue) {
        final Class<?> propertyValueClass = propertyValue.getClass();
        return String.class.isAssignableFrom(propertyValueClass) ||
                Long.class.isAssignableFrom(propertyValueClass) ||
                Integer.class.isAssignableFrom(propertyValueClass);
    }

    /**
     * Read Edges with the provided list of IDs.
     *
     * @param graph   Graph handle.
     * @param edgeIds Edge ids to read.
     * @return Edge.
     */
    public static List<FireflyEdge> readEdges(final FireflyGraph graph, final List<FireflyId> edgeIds) {
        final AerospikeConnection db = graph.getBaseGraph();

        final List<FireflyEdge> edges = new ArrayList<>();
        final Map<FireflyId, FireflyRecord> edgeRecords = FireflyRecord.batchReadPhatEdges(db, edgeIds);
        if (edgeRecords.isEmpty()) {
            return edges;
        }

        for (final FireflyId edgeId : edgeIds) {
            final FireflyRecord edgeRecord = edgeRecords.get(edgeId);
            if (edgeRecord != null) {
                final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
                if (edge != null) {
                    edges.add(edge);
                }
            }
        }

        return edges;
    }

    /**
     * Remove edge from Aerospike.
     *
     * Note: This does not remove the edge from its attached vertices.
     */
    public void removeEdge() {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, id);
        final List<Operation> operations = new ArrayList<>();

        final Operation removeEdgeData = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(id.getUserId()), MapReturnType.VALUE);
        operations.add(removeEdgeData);
        final Operation removeSupernodesIn = MapOperation.removeByKey(db.SUPERNODES_IN_BIN, Value.get(id.getUserId()), MapReturnType.NONE);
        operations.add(removeSupernodesIn);
        final Operation removeSupernodesOut = MapOperation.removeByKey(db.SUPERNODES_OUT_BIN, Value.get(id.getUserId()), MapReturnType.NONE);
        operations.add(removeSupernodesOut);
        if (db.isSupernodePushdownEnabled) {
            for (final Map.Entry<String, Object> property : this.properties.entrySet()) {
                final Object propertyValue = property.getValue();
                // Remove supernode properties that were pushed down
                if (isPropertyValuePushdownable(propertyValue)) {
                    appendRemoveFilterableSupernodePropertyOperation(this, property.getKey(), operations);
                }
            }
            // Remove adjacent Vertex ID and label pushdowns
            final Exp edgeUniqueId = Exp.val(((FireflyPhatEdgeId) this.id).getUniqueId());
            final Exp supernodePBinExp = Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN);
            final CTX labelMapKeyCtx = CTX.mapKey(Value.get(EDGE_SUPERNODE_LABEL_KEY));
            final int writeFlags = ExpWriteFlags.EVAL_NO_FAIL;
            if (this.isOutSupernode) {
                final Value outVIdValue = Value.get(this.outVid.getKeyHashString());
                final Exp outVIdExp = Exp.val(this.outVid.getKeyHashString());
                final Expression removeEdgeIdToLabelExp = Exp.build(
                        MapExp.removeByKey(edgeUniqueId, supernodePBinExp,
                                CTX.mapKey(outVIdValue), labelMapKeyCtx));
                final Operation removeEdgeIdToLabel = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN,
                        removeEdgeIdToLabelExp, writeFlags);
                operations.add(removeEdgeIdToLabel);
                if (db.isMergeEdgeDataModelEnabled) {
                    final Expression removeAdjacentVIdExp = Exp.build(
                            MapExp.removeByKey(edgeUniqueId, supernodePBinExp,
                                    CTX.mapKey(outVIdValue), CTX.mapKey(Value.get(EDGE_SUPERNODE_IN_KEY))));
                    final Operation removeAdjacentVId = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN,
                            removeAdjacentVIdExp, writeFlags);
                    operations.add(removeAdjacentVId);
                }
                // Remove entire vertex id key if label key is empty since that means there are no items
                final Expression removeVidKeyExp = Exp.build(
                        Exp.cond(
                                Exp.eq(MapExp.size(supernodePBinExp, CTX.mapKey(outVIdValue),
                                        labelMapKeyCtx), Exp.val(0)),
                                MapExp.removeByKey(outVIdExp, supernodePBinExp),
                                Exp.unknown()
                        )
                );
                final Operation removeVidKey = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN,
                        removeVidKeyExp, writeFlags);
                operations.add(removeVidKey);
            }
            if (this.isInSupernode) {
                final Value inVidValue = Value.get(this.inVid.getKeyHashString());
                final Exp inVidExp = Exp.val(this.inVid.getKeyHashString());
                final Expression removeEdgeIdToLabelExp = Exp.build(
                        MapExp.removeByKey(edgeUniqueId, supernodePBinExp,
                                CTX.mapKey(inVidValue), labelMapKeyCtx));
                final Operation removeEdgeIdToLabel = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN,
                        removeEdgeIdToLabelExp, writeFlags);
                operations.add(removeEdgeIdToLabel);
                if (db.isMergeEdgeDataModelEnabled) {
                    final Expression removeAdjacentVIdExp = Exp.build(
                            MapExp.removeByKey(edgeUniqueId, supernodePBinExp,
                                    CTX.mapKey(inVidValue), CTX.mapKey(Value.get(EDGE_SUPERNODE_OUT_KEY))));
                    final Operation removeAdjacentVId = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN,
                            removeAdjacentVIdExp, writeFlags);
                    operations.add(removeAdjacentVId);
                }
                // Remove entire vertex id key if label key is empty since that means there are no items
                final Expression removeVidKeyExp = Exp.build(
                        Exp.cond(
                                Exp.eq(MapExp.size(supernodePBinExp, CTX.mapKey(inVidValue),
                                        labelMapKeyCtx), Exp.val(0)),
                                MapExp.removeByKey(inVidExp, supernodePBinExp),
                                Exp.unknown()
                        )
                );
                final Operation removePropertyKey = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN,
                        removeVidKeyExp, writeFlags);
                operations.add(removePropertyKey);
            }
        }
        if (db.TTL_ENABLED_FLAG) {
            final Operation removeTtl = MapOperation.removeByKey(db.TTL_BIN, Value.get(id.getUserId()), MapReturnType.NONE);
            operations.add(removeTtl);
        }

        // Logic for deleting the entire phat edge record if it no longer contains individual edges.
        final Expression removeEmptyPhatEdgeExp = Exp.build(
                // If the size of the edge data map, which implicitly is the amount of edges in the phat edge, is 0,
                // write null. Otherwise, fail.
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(db.EDGE_DATA_BIN)), Exp.val(0)),
                        Exp.nil(),
                        Exp.unknown()
                )
        );
        // If all bins in a record contain null, the record is implicitly deleted.
        final int deletePhatEdgeWriteFlags = ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.ALLOW_DELETE;
        final Operation removeSupernodesInBin = ExpOperation.write(db.SUPERNODES_IN_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeSupernodesInBin);
        final Operation removeSupernodesOutBin = ExpOperation.write(db.SUPERNODES_OUT_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeSupernodesOutBin);
        if (db.isSupernodePushdownEnabled) {
            final Operation removeSupernodePropertiesBin = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
            operations.add(removeSupernodePropertiesBin);
        }
        if (db.TTL_ENABLED_FLAG) {
            final Operation removeTtlBin = ExpOperation.write(db.TTL_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
            operations.add(removeTtlBin);
        }

        // This operation must be last since the expression checks the map in the edge data bin.
        final Operation removeEdgeDataBin = ExpOperation.write(db.EDGE_DATA_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeEdgeDataBin);

        final WritePolicy policy = new WritePolicy();
        if (db.isSupernodePushdownEnabled && (this.isInSupernode || this.isOutSupernode)) {
            policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            policy.generation = this.generation;
        }
        try {
            final Record record = db.writeOperate(policy, key, operations.toArray(new Operation[0]));

            // Result returned is always [List<?>, null] since we have operations [removeEdgeData, removeEdgeDataBin]
            final Command.OpResults results = (Command.OpResults) record.getValue(db.EDGE_DATA_BIN);
            if (results != null && !results.isEmpty()) {
                final Object edgeData = results.get(0);
                // Check edgeData value was returned to protect against concurrent deletes.
                // If this Edge was already removed edgeData returns null and this check returns false.
                if (edgeData instanceof List) {
                    graph.edgeIdManager.recycleId(id);
                    final String label = (String) ((List<?>) edgeData).get(LABEL_POSITION);
                    graph.fireflySummaryUpdater.addEdgeRemoveToQueue(label);
                } else if (edgeData != null) {
                    // This should never happen.
                    throw new RuntimeException("Individual Edge data in Phat Edge returned as type that is of type: " + edgeData.getClass());
                }
                LOG.debug("Ignoring exception when deleting Edge with id " + getUserIdString(id.getUserId()) + " since it was not found.");
            }
        } catch (final ElementNotFoundException e) {
            // This tends to occur when deleting multiple vertices in a single traversal where the Edge lives in between
            // the to-be-deleted vertices.
            LOG.debug("Ignoring exception when deleting Edge with id " + getUserIdString(id.getUserId()) + " since it was not found.");
        } catch (final AerospikeException e) {
            if (e.getResultCode() != ResultCode.GENERATION_ERROR) {
                throw e;
            }
            LOG.debug("Regenerating supernode Edge with id " + getUserIdString(id.getUserId()) + "to clean up supernode property bin.");
            final List<FireflyEdge> edges = readEdges(graph, List.of(this.id));
            // If no edges come back that's okay, because it means a different traversal has cleaned up this edge.
            if (!edges.isEmpty()) {
                if (edges.size() > 1) {
                    // This should never happen.
                    final String message = "Regeneration of supernode edge during delete returned multiple edges for a single id: " + getUserIdString(id.getUserId());
                    LOG.error(message);
                    throw new IllegalStateException(message);
                }
                edges.get(0).removeEdge();
            }
            LOG.debug("Generation check retry failed when regenerating Edge with id " + getUserIdString(id.getUserId()) + " since it was not found.");
        }
        this.removed = true;
    }

    @Override
    public Object id() {
        return getBase64UserIdString(this.id);
    }

    static private String getBase64UserIdString(final FireflyId id) {
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

    public FireflyId outVertexId() {
        return outVid;
    }

    public FireflyId inVertexId() {
        return inVid;
    }

    @Override
    public Vertex outVertex() {
        // TODO GRAPH-1145: Restore the following and handle skipping null returns.
        //return graph.readVertex(this.outVid);
        return getSingleVertex(this.outVid);
    }

    @Override
    public Vertex inVertex() {
        // TODO GRAPH-1145: Restore the following and handle skipping null returns.
        //return graph.readVertex(this.inVid);
        return getSingleVertex(this.inVid);
    }

    /**
     * Get a Vertex directly via ID, bypassing the TTL pushdown filter.
     *
     * @param vertexId ID of Vertex to return
     * @return  Vertex
     */
    private FireflyVertex getSingleVertex(final FireflyId vertexId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, vertexId);
        if (fireflyRecord == null) {
            return null;
        }
        final KeyRecord keyRecord = new KeyRecord(fireflyRecord.key(), fireflyRecord.record());
        return FireflyVertex.fromRecord(graph, keyRecord);
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
            return new FireflyEdgeProperty<>(graph, this, key, casted);
        } else {
            return Property.empty();
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        FireflyHelper.legalPropertyKeyValueArray(key, value);

        // Edge is already removed.
        if (this.removed) {
            throw elementAlreadyRemoved(Edge.class, id);
        }

        // Handle TTL.
        if (TTL_PROPERTY_KEY.equals(key)) {
            if (!db.TTL_ENABLED_FLAG) {
                throw new TtlNotEnabledException();
            }
            if (Number.class.isAssignableFrom(value.getClass())) {
                setTtl(((Number) value).longValue());
                return Property.empty();
            } else {
                throw new IllegalArgumentException(
                        String.format("Property value [%s] for key %s is of type %s and must be numeric", value, key,
                                value.getClass()));
            }
        }

        // Cannot be hidden key.
        if (isHidden(key))
            throw Property.Exceptions.propertyKeyCanNotBeAHiddenKey(key);

        // Remove the property.
        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            properties.remove(key);
            typeHints.remove(key);
            return Property.empty();
        }

        // Write the property and add to edge.
        FireflyHelper.validatePropertyValue(value);
        return writeProperty(graph, this, key, value);
    }

    @Override
    public void remove() {
        //@todo multi record transactions
        // Until we have MRT support, we must remove the edge record itself first, then
        // remove the edge from the individual vertices.
        // But doing this, should one of the subsequent deletes fail, we will not have an
        // orphaned edge on one vertex but not the other.
        removeEdge();
        removeFromOut();
        removeFromIn();
        if (graph.getBaseGraph().IS_AUDIT_LOG_ENABLED) {
            LOG.info("[{}] Dropped edge [{}]-[{}]>[{}].", graph.getUser(), outVertex().id(), label, inVertex().id());
        }
    }

    public void removeSelfAndFromOut() {
        removeEdge();
        removeFromOut();
    }

    private void removeFromOut() {
        // If OUT is a supernode it means this Edge does not exist on its record so skip reading it.
        if (!this.isOutSupernode) {
            final FireflyVertex outVertex = this.graph.readVertex(this.outVid);
            if (outVertex != null) {
                final FireflyIdFactory idFactory = this.graph.getIdFactory();
                outVertex.removeEdge(Direction.OUT, (FireflyPhatEdgeId) this.id, this.label);
            }
        }
    }

    public void removeSelfAndFromIn() {
        removeEdge();
        removeFromIn();
    }

    private void removeFromIn() {
        // If IN is a supernode it means this Edge does not exist on its record so skip reading it.
        if (!this.isInSupernode) {
            final FireflyVertex inVertex = this.graph.readVertex(this.inVid);
            if (inVertex != null) {
                final FireflyIdFactory idFactory = this.graph.getIdFactory();
                inVertex.removeEdge(Direction.IN, (FireflyPhatEdgeId) this.id, this.label);
            }
        }
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
                return FireflyCloseableIteratorUtils.of(new FireflyEdgeProperty<>(graph, this, propertyKeys[0],
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
                    propertyList.add(new FireflyEdgeProperty<>(graph, this, key,
                            (V) this.graph.getBaseGraph().convertValuetoTypeUsingHint(
                                    properties.get(key), typeHints.get(key))));
                }
            }
            return propertyList.iterator();
        }
    }

    public static <V> Property<V> writeProperty(final FireflyGraph graph, final FireflyEdge edge,
                                                final String propertyKey, final V value) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final Value edgeIdMapKey = Value.get(edge.id.getUserId());

        final List<Operation> operations = new ArrayList<>();

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation valueOp = MapOperation.put(policy, db.EDGE_DATA_BIN, Value.get(propertyKey), Value.get(value),
                CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
        operations.add(valueOp);
        final Object typeHint = getTypeHintOf(value);
        if (typeHint != null) {
            final Operation typeHintOp = MapOperation.put(policy, db.EDGE_DATA_BIN, Value.get(propertyKey),
                    Value.get(typeHint), CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
            operations.add(typeHintOp);
        }

        // If this property is not able to be pushed down, make sure we clean it up.
        if (!isPropertyValuePushdownable(value)) {
            appendRemoveFilterableSupernodePropertyOperation(edge, propertyKey, operations);
        }

        appendFilterableSupernodePropertyOperation(graph, (FireflyPhatEdgeId) edge.id, edge.isOutSupernode,
                edge.isInSupernode, edge.outVid, edge.inVid, propertyKey, value, operations);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));
            edge.properties.put(propertyKey, value);
            if (typeHint != null) {
                edge.typeHints.put(propertyKey, typeHint);
            }
            graph.fireflySummaryUpdater.addEdgePropertiesWriteToQueue(edge.label, Set.of(propertyKey));
            return new FireflyEdgeProperty<>(graph, edge, propertyKey, value);
        } catch (final RecordTooBigException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingProperty((AerospikeException) e.getCause(), db, key, edge.id, propertyKey);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        } catch (final AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case
                // the key is the Phat Edge key and thus the key still exists.
                throw new ElementNotFoundException(edge, ae);
            } else {
                throw ae;
            }
        }
    }

    private void setTtl(final long durationSeconds) {
        final Key key = getKey(this.db, this.db.EDGE_AERO_SET, this.id);
        final long expirationTime = System.currentTimeMillis() + (durationSeconds * 1000);
        final Value edgeIdMapKey = Value.get(this.id.getUserId());

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation writeTtl = MapOperation.put(policy, db.TTL_BIN, edgeIdMapKey, Value.get(expirationTime));

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, key, writeTtl);
        } catch (final RecordTooBigException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingProperty((AerospikeException) e.getCause(), db, key, this.id, TTL_PROPERTY_KEY);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case
                // the key is the Phat Edge key and thus the key still exists.
                throw new ElementNotFoundException(this, ae);
            } else {
                throw ae;
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public static class FireflyEdgeFactory {
        private static FireflyEdge create(final FireflyId fid, final String label, final FireflyGraph graph,
                                          final FireflyId outVertex, final FireflyId inVertex,
                                          final Map<String, Object> properties, final Map<String, Object> typeHints,
                                          final boolean isOutSupernode, final boolean isInSupernode, final int generation) {
            final FireflyPhatEdgeId edgeId;
            if (fid instanceof FireflyIdComposite) {
                edgeId = ((FireflyIdComposite) fid).getEdgeId();
            } else {
                edgeId = (FireflyPhatEdgeId) fid;
            }
            return new FireflyEdge(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, generation);
        }

        private static FireflyEdge create(final FireflyId edgeId, final FireflyRecord fireflyRecord,
                                             final FireflyGraph graph) {
            if (fireflyRecord == null) {
                return null;
            }
            return create(edgeId, fireflyRecord.record(), graph);
        }

        public static FireflyEdge create(final FireflyId edgeId, final Record record, final FireflyGraph graph) {
            if (record == null) {
                return null;
            }
            final AerospikeConnection db = graph.getBaseGraph();
            final ByteBuffer edgeIdMapKey = (ByteBuffer) edgeId.getUserId();
            final Map<ByteBuffer, List> edgeData = (Map<ByteBuffer, List>) record.getMap(db.EDGE_DATA_BIN);
            // Implicitly assume that if the key is found for label, which is required, then the key exists for the
            // other phat edge maps, since they are all written in the same operate.
            if (!edgeData.containsKey(edgeIdMapKey)) {
                return null;
            }
            final String label = (String) edgeData.get(edgeIdMapKey).get(LABEL_POSITION);

            final String outV = (String) edgeData.get(edgeIdMapKey).get(OUT_V_POSITION);
            final FireflyId outVertex = FireflyIdPoly.fromHashString(outV, db.VERTEX_AERO_SET);

            final String inV = (String) edgeData.get(edgeIdMapKey).get(IN_V_POSITION);
            final FireflyId inVertex = FireflyIdPoly.fromHashString(inV, db.VERTEX_AERO_SET);

            final Map<String, Object> properties = (Map<String, Object>) edgeData.get(edgeIdMapKey).get(PROPERTIES_POSITION);
            final Map<String, Object> typeHints = (Map<String, Object>) edgeData.get(edgeIdMapKey).get(TYPE_HINTS_POSITION);

            final Map<ByteBuffer, String> outSupernodes = (Map<ByteBuffer, String>) record.getMap(graph.getBaseGraph().SUPERNODES_OUT_BIN);
            final Map<ByteBuffer, String> inSupernodes = (Map<ByteBuffer, String>) record.getMap(graph.getBaseGraph().SUPERNODES_IN_BIN);
            final boolean isOutSupernode = outSupernodes != null && outSupernodes.containsKey(edgeId.getUserId());
            final boolean isInSupernode = inSupernodes != null && inSupernodes.containsKey(edgeId.getUserId());

            return create(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, record.generation);
        }
    }
}
