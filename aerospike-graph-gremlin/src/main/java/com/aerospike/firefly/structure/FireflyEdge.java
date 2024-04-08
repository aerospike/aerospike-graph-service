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
import com.aerospike.client.cdt.MapWriteMode;
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
import com.aerospike.firefly.runtime.exceptions.TtlNotEnabledException;
import com.aerospike.firefly.structure.id.FireflyId;
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

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
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
    public static final String EDGE_SUPERNODE_ID_KEY = T.id.getAccessor();
    public static final String EDGE_SUPERNODE_LABEL_KEY = T.label.getAccessor();

    protected final AerospikeConnection db;
    public boolean removed;
    protected final FireflyGraph graph;
    protected final FireflyId inVid;
    protected final FireflyId outVid;
    protected final Map<String, Object> properties;
    protected final Map<String, Object> typeHints;
    private final boolean isInSupernode;
    private final boolean isOutSupernode;

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
        final MapPolicy edgeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY);

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
        boolean scheduleTtlImmediately = false;
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
                if (ttlValueLong < db.TTL_PURGE_INTERVAL_SECONDS) {
                    scheduleTtlImmediately = true;
                }
            } else {
                throw new IllegalArgumentException(
                        String.format("Property value [%s] for key %s is of type %s and must be numeric", ttlValue,
                                TTL_PROPERTY_KEY, ttlValue.getClass()));
            }
        }

        // Write to filterable supernode bin if necessary.
        operations.addAll(createFilterableSupernodeOperation(graph, (FireflyPhatEdgeId) edgeId, !outVertexCacheWrite,
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
            db.operate(writePolicy, key, operations.toArray(new Operation[0]));
            graph.fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
            final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id,
                    propertyMap, typeHints, !outVertexCacheWrite, !inVertexCacheWrite);
            if (scheduleTtlImmediately) {
                graph.scheduleElementForTtlNow(edge, ttlValueLong);
            }
            return edge;
        } catch (final RecordTooBigException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingEdge((AerospikeException) e.getCause(), db, key, edgeId);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    public static List<Operation> createFilterableSupernodeOperation(final FireflyGraph graph, final FireflyPhatEdgeId edgeId,
                                                                final boolean isOutSupernode, final boolean isInSupernode,
                                                                final FireflyId outVId, final FireflyId inVId,
                                                                final String label, final Map<String, Object> propertyMap) {
        if (!isOutSupernode && !isInSupernode) {
            // No supernodes so we don't have to do anything
            return Collections.emptyList();
        }
        final String binName = graph.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN;
        final Value edgeStorageIndex = Value.get(edgeId.getPackingIndex());
        final Value outVIdValue = Value.get(outVId.getKeyHashString());
        final Value inVIdValue = Value.get(inVId.getKeyHashString());

        final List<Operation> operations = new ArrayList<>();
        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        // ID and Label
        if (isOutSupernode) {
            final Operation idOperation = MapOperation.put(policy, binName, edgeStorageIndex, Value.get(getBase64UserIdString(edgeId)),
                    CTX.mapKeyCreate(outVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_ID_KEY), MapOrder.KEY_ORDERED));
            operations.add(idOperation);
            final Operation labelOperation = MapOperation.put(policy, binName, edgeStorageIndex, Value.get(label),
                    CTX.mapKeyCreate(outVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_LABEL_KEY), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
        }
        if (isInSupernode) {
            final Operation idOperation = MapOperation.put(policy, binName, edgeStorageIndex, Value.get(getBase64UserIdString(edgeId)),
                    CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_ID_KEY), MapOrder.KEY_ORDERED));
            operations.add(idOperation);
            final Operation labelOperation = MapOperation.put(policy, binName, edgeStorageIndex, Value.get(label),
                    CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_LABEL_KEY), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
        }
        // Properties
        for (final Map.Entry<String, Object> property : propertyMap.entrySet()) {
            final Class<?> propertyValueClass = property.getValue().getClass();
            if (String.class.isAssignableFrom(propertyValueClass) ||
                    Long.class.isAssignableFrom(propertyValueClass) ||
                    Integer.class.isAssignableFrom(propertyValueClass)) {
                if (isOutSupernode) {
                    final Operation propertyOperation = MapOperation.put(policy, binName, edgeStorageIndex, Value.get(property.getValue()),
                            CTX.mapKeyCreate(outVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(property.getKey()), MapOrder.KEY_ORDERED));
                    operations.add(propertyOperation);
                }
                if (isInSupernode) {
                    final Operation propertyOperation = MapOperation.put(policy, binName, edgeStorageIndex, Value.get(property.getValue()),
                            CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(property.getKey()), MapOrder.KEY_ORDERED));
                    operations.add(propertyOperation);
                }
            }
        }
        return operations;
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

        final Operation removeEdgeData = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(edgeId.getUserId()), MapReturnType.VALUE);
        final Operation removeSupernodesIn = MapOperation.removeByKey(db.SUPERNODES_IN_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeSupernodesOut = MapOperation.removeByKey(db.SUPERNODES_OUT_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);
        final Operation removeTtl = MapOperation.removeByKey(db.TTL_BIN, Value.get(edgeId.getUserId()), MapReturnType.NONE);

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
        final Operation removeSupernodesOutBin = ExpOperation.write(db.SUPERNODES_OUT_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeTtlBin = ExpOperation.write(db.TTL_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        final Operation removeSupernodePropertiesBin = ExpOperation.write(db.SUPERNODE_EDGE_PROPERTIES_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);

        // This operation must be last since the expression checks the map in the edge data bin.
        final Operation removeEdgeDataBin = ExpOperation.write(db.EDGE_DATA_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);

        try {
            final Record record = db.operate(null, key, removeEdgeData, removeSupernodesIn,
                    removeSupernodesOut, removeTtl, removeSupernodesInBin, removeSupernodesOutBin, removeTtlBin,
                    removeSupernodePropertiesBin, removeEdgeDataBin);

            // Result returned is always [List<?>, null] since we have operations [removeEdgeData, removeEdgeDataBin]
            final Command.OpResults results = (Command.OpResults) record.getValue(db.EDGE_DATA_BIN);
            if (results != null && !results.isEmpty()) {
                final Object edgeData = results.get(0);
                // Check edgeData value was returned to protect against concurrent deletes.
                // If this Edge was already removed edgeData returns null and this check returns false.
                if (edgeData instanceof List) {
                    graph.edgeIdManager.recycleId(edgeId);
                    final String label = (String) ((List<?>) edgeData).get(LABEL_POSITION);
                    graph.fireflySummaryUpdater.addEdgeRemoveToQueue(label);
                } else if (edgeData != null) {
                    // This should never happen.
                    throw new RuntimeException("Individual Edge data in Phat Edge returned as type that is of type: " + edgeData.getClass());
                }
                LOG.debug("Ignoring exception when deleting Edge with id " + getUserIdString(edgeId.getUserId()) + " since it was not found.");
            }
        } catch (final ElementNotFoundException e) {
            // This tends to occur when deleting multiple vertices in a single traversal where the Edge lives in between
            // the to-be-deleted vertices.
            LOG.debug("Ignoring exception when deleting Edge with id " + getUserIdString(edgeId.getUserId()) + " since it was not found.");
        }
    }

    /**
     * Remove edge from Aerospike.
     */
    public void removeEdge() {
        FireflyEdge.removeEdgeById(this.graph, this.id);
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

    public FireflyEdge(final FireflyId id,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId outVid,
                       final FireflyId inVid,
                       final Map<String, Object> properties,
                       final Map<String, Object> typeHints,
                       final boolean isOutSupernode,
                       final boolean isInSupernode) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
        this.properties = properties;
        this.typeHints = typeHints;
        this.db = graph.getBaseGraph();
        this.isOutSupernode = isOutSupernode;
        this.isInSupernode = isInSupernode;
    }

    @Override
    public Vertex outVertex() {
        return graph.readVertex(this.outVid);
    }

    @Override
    public Vertex inVertex() {
        return graph.readVertex(this.inVid);
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
        final Property<V> property = writeProperty(graph, this, key, value);
        properties.put(key, value);
        final Object typeHint = getTypeHintOf(value);
        if (typeHint != null) {
            typeHints.put(key, typeHint);
        }
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
        removeFromOut();
        removeFromIn();
    }

    public void removeSelfAndFromOut() {
        removeEdge();
        removeFromOut();
    }

    private void removeFromOut() {
        final FireflyVertex outVertex = this.graph.readVertex(this.outVid);
        if (outVertex != null) {
            final FireflyIdFactory idFactory = this.graph.getIdFactory();
            outVertex.removeEdge(Direction.OUT, idFactory.createCompositeEdgeId(this.id, this.inVid), this.label);
        }
    }

    public void removeSelfAndFromIn() {
        removeEdge();
        removeFromIn();
    }

    private void removeFromIn() {
        final FireflyVertex inVertex = this.graph.readVertex(this.inVid);
        if (inVertex != null) {
            final FireflyIdFactory idFactory = this.graph.getIdFactory();
            inVertex.removeEdge(Direction.IN, idFactory.createCompositeEdgeId(this.id, this.outVid), this.label);
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

    public static <V> Property<V> writeProperty(final FireflyGraph graph, final FireflyEdge edge, final String propertyKey, final V value) {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final Value edgeIdMapKey = Value.get(edge.id.getUserId());

        final List<Operation> operations = new ArrayList<>();
        final Operation valueOp;

        // Null value properties are not currently supported by Firefly and thus the correct behaviour is to remove
        // the property key if a null value is given.
        if (value == null) {
            valueOp = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
            operations.add(valueOp);
            final Operation typeHintOp = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
            operations.add(typeHintOp);
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            valueOp = MapOperation.put(policy, db.EDGE_DATA_BIN, Value.get(propertyKey), Value.get(value),
                    CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
            operations.add(valueOp);
            final Object typeHint = getTypeHintOf(value);
            if (typeHint != null) {
                final Operation typeHintOp = MapOperation.put(policy, db.EDGE_DATA_BIN, Value.get(propertyKey),
                        Value.get(typeHint), CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
                operations.add(typeHintOp);
            }
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.operate(writePolicy, key, operations.toArray(new Operation[0]));
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

        graph.fireflySummaryUpdater.addEdgePropertiesWriteToQueue(edge.label, Set.of(propertyKey));
        return new FireflyEdgeProperty<>(graph, edge, propertyKey, value);
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
            db.operate(writePolicy, key, writeTtl);
            if (durationSeconds < db.TTL_PURGE_INTERVAL_SECONDS) {
                this.graph.scheduleElementForTtlNow(this, durationSeconds);
            }
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
    public long getTtlMillis() {
        final Key key = getKey(this.db, this.db.EDGE_AERO_SET, this.id);
        final Operation getTtlBin = Operation.get(this.db.TTL_BIN);
        try {
            final Record result = this.db.operate(null, key, getTtlBin);
            final Map<Object, Long> ttlMap = (Map<Object, Long>) result.getMap(this.db.TTL_BIN);
            final Long expiryTime = ttlMap.get(this.id.getUserId());
            if (expiryTime == null) {
                // Edge was already deleted but phat Edge record still exists.
                throw new ElementNotFoundException();
            }
            return expiryTime - System.currentTimeMillis();
        } catch (final AerospikeException e) {
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                // Edge was already deleted.
                throw new ElementNotFoundException(e);
            }
            LOG.error("Unexpected error when checking TTL for Edge " + this.id(), e);
            throw e;
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
                                          final boolean isOutSupernode, final boolean isInSupernode) {
            return new FireflyEdge(fid, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode);
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

            return create(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode);
        }
    }
}
