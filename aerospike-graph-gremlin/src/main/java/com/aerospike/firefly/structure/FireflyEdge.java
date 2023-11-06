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
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException;
import com.aerospike.firefly.runtime.exceptions.ElementNotFoundException;
import com.aerospike.firefly.runtime.exceptions.RecordTooBigException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
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

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.fromAddingEdge;
import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.fromAddingProperty;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyEdge extends FireflyElement implements Edge {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyEdge.class);
    protected final AerospikeConnection db;
    public boolean removed;
    protected final FireflyGraph graph;
    protected final FireflyId inVid;
    protected final FireflyId outVid;
    protected final Map<String, Object> properties;
    protected final Map<String, Object> typeHints;

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

/*
We must have a standard for if inV or outV occurs first
the subtle issue here is that both of the 4-5th arguments have the same Type but are reversed in order.
    public FireflyEdge(final FireflyId fid,
                          final String label,
                          final FireflyGraph graph,
                          final FireflyId outVertex,
                          final FireflyId inVertex,
                          final Map<String, Object> properties,
                          final Map<String, Object> typeHints) {
    public FireflyEdge(final FireflyId id,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId inVid,
                       final FireflyId outVid,
                       final Map<String, Object> properties,
                       final Map<String, Object> typeHints)

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
        final Key key = getKey(db, db.EDGE_AERO_SET, edgeId);
        try {
            db.operate(writePolicy, key, operations.toArray(new Operation[0]));
            graph.fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
            return FireflyEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id, data, typeHints);
        } catch (final RecordTooBigException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingEdge((AerospikeException) e.getCause(), db, key, edgeId);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
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
                final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
                if (edge != null) {
                    edges.add(edge);
                }
            }
        }

        return edges;
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

        final Operation removeLabel = MapOperation.removeByKey(db.LABEL_BIN, Value.get(edgeId.getUserId()), MapReturnType.VALUE);
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
            final Record record = db.operate(null, key, removeLabel, removeIn, removeOut, removeProperties, removeTypeHints,
                    removeSupernodesIn, removeSupernodesOut, removeInBin, removeOutBin, removePropertiesBin,
                    removeTypeHintsBin, removeSupernodesInBin, removeSupernodesOutBin, removeLabelBin);
            graph.edgeIdManager.recycleId(edgeId);

            // Result returned is always [<label>, null] since the label is removed first.
            final Command.OpResults results = (Command.OpResults) record.getValue(db.LABEL_BIN);
            for (int i = 0; i < results.size(); i++) {
                final Object result = results.get(i);
                if (result instanceof String) {
                    graph.fireflySummaryUpdater.addEdgeRemoveToQueue((String) result);
                    break;
                }
            }
        } catch (final ElementNotFoundException e) {
            // This tends to occur when deleting multiple vertices in a single traversal where the Edge lives in between
            // the to-be-deleted vertices.
            LOG.info("Ignoring exception when deleting Edge with id " + edgeId.getUserId() + " since it was not found.");
        }
    }

    /**
     * Remove edge from Aerospike.
     */
    public void removeEdge() {
        FireflyEdge.removeEdgeById(this.graph, this.id);
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

    public FireflyEdge(final FireflyId id,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId outVid,
                       final FireflyId inVid,
                       final Map<String, Object> properties,
                       final Map<String, Object> typeHints) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
        this.properties = properties;
        this.typeHints = typeHints;
        this.db = graph.getBaseGraph();
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
            return new FireflyEdgeProperty<>(graph, this, key, casted);
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

    public static <V> Property<V> writeProperty(final FireflyGraph graph, final FireflyEdge edge, final String propertyKey, final V value) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final Value edgeIdMapKey = Value.get(edge.id.getUserId());

        final Operation valueOp;
        final Operation typeHintOp;

        // Null value properties are not currently supported by Firefly and thus the correct behaviour is to remove
        // the property key if a null value is given.
        if (value == null) {
            valueOp = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(edgeIdMapKey));
            typeHintOp = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(edgeIdMapKey));
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            valueOp = MapOperation.put(policy, db.PROPERTIES_BIN, Value.get(propertyKey), Value.get(value),
                    CTX.mapKey(edgeIdMapKey));
            typeHintOp = MapOperation.put(policy, db.TYPE_HINTS_BIN, Value.get(propertyKey),
                    Value.get(getSupportedType(value)), CTX.mapKey(edgeIdMapKey));
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.operate(writePolicy, key, valueOp, typeHintOp);
        } catch (final RecordTooBigException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingProperty((AerospikeException) e.getCause(), db, key, edge.id, propertyKey);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
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
        return new FireflyEdgeProperty<>(graph, edge, propertyKey, value);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    private static class FireflyEdgeFactory {
        private static FireflyEdge create(final FireflyId fid, final String label, final FireflyGraph graph,
                                             final FireflyId outVertex, final FireflyId inVertex,
                                             final Map<String, Object> properties, final Map<String, Object> typeHints) {
            return new FireflyEdge(fid, label, graph, outVertex, inVertex, properties, typeHints);
        }

        private static FireflyEdge create(final FireflyId edgeId, final FireflyRecord fireflyRecord,
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

            return FireflyEdge.FireflyEdgeFactory.create(edgeId, label, graph, outVertex, inVertex, properties, typeHints);
        }
    }
}
