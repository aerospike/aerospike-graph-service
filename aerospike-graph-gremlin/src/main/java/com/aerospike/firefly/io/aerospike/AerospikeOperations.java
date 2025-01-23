package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
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
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyEdgeProperty;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexFactory;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.FireflyVertexPropertyProperty;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.id.LazyIdTransform;
import com.aerospike.firefly.structure.id.LazyVertexPropertyIdTransform;
import com.aerospike.firefly.structure.iterator.FireflyBatchEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphElementNotFoundException;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.AerospikeGraphRecordSizeExceededException;
import com.aerospike.firefly.util.exceptions.EdgeRecordSizeExceededException;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.io.aerospike.OperationReturnHandler.getValueAtIndex;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_DATA_SIZE;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_IN_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_OUT_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;
import static com.aerospike.firefly.structure.FireflyElement.TTL_PROPERTY_KEY;
import static com.aerospike.firefly.util.FireflyHelper.validatePropertyValue;
import static com.aerospike.firefly.util.exceptions.EdgeRecordSizeExceededException.fromAddingEdge;
import static com.aerospike.firefly.util.exceptions.EdgeRecordSizeExceededException.fromAddingProperty;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.fromAddingToEdgeCache;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.fromAddingVertexProperty;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.fromAddingVpProperty;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.getRelevantVertexBins;

public class AerospikeOperations {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeOperations.class);

    // next fields `protected` for tests
    protected final AerospikeConnection db;
    protected final FireflyGraph graph;

    public AerospikeOperations(final FireflyGraph graph) {
        this.db = graph.getBaseGraph();
        this.graph = graph;
    }

    protected Txn getOrCreateTxn() {
        // 1. try get txn from graph (when FireflyGraph will support tx)
        // 2. if db has MRT enabled, then create new txn
        // 3. else no txn support
        if (!db.MRT_ENABLED) return null;

        final Txn txn = new Txn();
        txn.setTimeout(db.MRT_TIMEOUT);
        return txn;
    }

    //////////////// VERTEX OPERATIONS ///////////////


    /**
     * Write and construct a FireflyVertex using the provided parameters.
     *
     * @param vertexId              Id of vertex.
     * @param label                 String label of vertex.
     * @param properties            Map of properties to add to vertex.
     * @param createOnly            Flag that allows only new IDs to be written. Disable only for retry purposes.
     * @param isEdgeCacheOverflowed Initial state of edge cache to set.
     * @return FireflyVertex.
     */
    public FireflyVertex writeVertex(final FireflyId vertexId,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties,
                                     final int vertexTypeHint,
                                     final boolean createOnly,
                                     final boolean isEdgeCacheOverflowed) {
        LOG.debug("Writing Vertex {} {}.", vertexId, properties);

        final Map<String, FireflyId> vertexPropertyIds;
        final Map<String, ?> vertexPropertyIdsWritable;
        final Map<String, Object> vertexPropertyValueMap;

        // TODO GRAPH-301: This works fine for now since Linked is being deprecated so multi-properties isn't a concern.
        //                 The idea is that a null property value is supposed to remove the key if one exists so we
        //                 search for them in one pass to find the last invalid index, and then do a second pass to get
        //                 the valid properties.
        final Map<String, Integer> lastNullIndexes = new HashMap<>();
        final List<Map.Entry<String, Object>> validProperties = new ArrayList<>();
        for (int i = 0; i < properties.size(); i++) {
            final Map.Entry<String, Object> property = properties.get(i);
            if (property.getValue() == null) {
                lastNullIndexes.put(property.getKey(), i);
            }
        }
        for (int i = 0; i < properties.size(); i++) {
            final Map.Entry<String, Object> property = properties.get(i);
            // If the property is null, obviously don't need it to the list of valid properties.
            if (property.getValue() == null) {
                continue;
            }
            // If there is no instance of a null value with this key we can add it safely.
            // If there is an instance of a null valid, it is safe to add as long as it exists past the last found index.
            if (!lastNullIndexes.containsKey(property.getKey()) || i > lastNullIndexes.get(property.getKey())) {
                validProperties.add(new AbstractMap.SimpleEntry<>(property.getKey(), validatePropertyValue(property.getValue())));
            }
        }

        final List<Operation> operations = new ArrayList<>();
        long ttlValueLong;
        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {
            final FireflyVertex.PropertyValueIdMaps propertyValueIdMaps = getPropertyValueIdMaps(validProperties);
            // Handle special TTL property if flag is enabled.
            if (propertyValueIdMaps.valueMap.containsKey(TTL_PROPERTY_KEY)) {
                if (!db.TTL_ENABLED_FLAG) {
                    throw new AerospikeGraphException(GraphError.TTL_NOT_ENABLED);
                }
                final Object ttlValue = propertyValueIdMaps.valueMap.remove(TTL_PROPERTY_KEY);
                propertyValueIdMaps.idMap.remove(TTL_PROPERTY_KEY);
                if (Number.class.isAssignableFrom(ttlValue.getClass())) {
                    ttlValueLong = ((Number) ttlValue).longValue();
                    final long expirationTime = System.currentTimeMillis() + (ttlValueLong * 1000);
                    final Bin ttlBin = new Bin(db.TTL_BIN, expirationTime);
                    final Operation writeTtlBin = Operation.put(ttlBin);
                    operations.add(writeTtlBin);
                } else {
                    throw new IllegalArgumentException(
                            String.format("Property value [%s] for key %s is of type %s and must be numeric", ttlValue,
                                    TTL_PROPERTY_KEY, ttlValue.getClass()));
                }
            }
            vertexPropertyIds = propertyValueIdMaps.idMap;
            vertexPropertyIdsWritable = graph.getIdFactory().convertMapToStorage(propertyValueIdMaps.idMap);
            vertexPropertyValueMap = propertyValueIdMaps.valueMap;
        } else {
            // Should never happen.
            throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }

        // Create vertex bins for cache state, vertex label, and property ids.
        final Bin cacheDisabledBin = new Bin(db.EDGE_CACHE_DISABLED_BIN, Value.get(isEdgeCacheOverflowed));
        final Operation writeCacheDisabled = Operation.put(cacheDisabledBin);
        final Bin labelBin = new Bin(db.LABEL_BIN, Value.get(label));
        final Operation writeLabel = Operation.put(labelBin);
        final Bin typeHintBin = new Bin(db.RELATIONAL_VERTEX_TYPE_HINT_BIN, Value.get(vertexTypeHint));
        final Operation writeTypeHint = Operation.put(typeHintBin);
        final Map<String, List<Long>> emptyEdgeCache = new TreeMap<>();
        final Bin edgeCacheInBin = new Bin(db.IN_EDGES_BIN, Value.get(emptyEdgeCache, MapOrder.KEY_ORDERED));
        final Operation writeEdgeCacheIn = Operation.put(edgeCacheInBin);
        final Bin edgeCacheOutBin = new Bin(db.OUT_EDGES_BIN, Value.get(emptyEdgeCache, MapOrder.KEY_ORDERED));
        final Operation writeEdgeCacheOut = Operation.put(edgeCacheOutBin);

        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {
            final Bin vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID_BIN,
                    Value.get(vertexPropertyIdsWritable, MapOrder.KEY_ORDERED));
            final Operation writeVertexPropertyIds = Operation.put(vertexPropertyIdsBin);
            final Bin vertexPropertyValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                    Value.get(vertexPropertyValueMap, MapOrder.KEY_ORDERED));
            final Operation writeVertexPropertyValues = Operation.put(vertexPropertyValuesBin);
            final Map<String, Object> vertexPropertyTypeHintMap = new TreeMap<>();
            for (Map.Entry<String, ?> entry : vertexPropertyValueMap.entrySet()) {
                final Object typeHint = getTypeHintOf(entry.getValue());
                if (typeHint != null) {
                    vertexPropertyTypeHintMap.put(entry.getKey(), typeHint);
                }
            }
            final Bin vertexPropertyValuesTypeHintsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN,
                    Value.get(vertexPropertyTypeHintMap, MapOrder.KEY_ORDERED));
            final Operation writeVertexPropertyTypeHints = Operation.put(vertexPropertyValuesTypeHintsBin);

            // Create Vertex Property Properties maps.
            // In the Packed model, a Vertex Property's details are stored in the same record as the Vertex itself.
            // Thus, the Vertex Property's Properties are also saved on the Vertex's record in Bins which map the
            // Vertex Property ID to a map of Key-Value pairs that represents the Vertex Property's Properties.
            // The existence of the Vertex Property ID as a key in this map is what is used to determine whether the
            // Vertex Property currently exists, and thus instantiating it here is necessary.
            final Map<Object, Map<String, Object>> vpProperties = new TreeMap<>();
            final Map<Object, Map<String, Object>> vpPropertiesTypeHints = new TreeMap<>();
            for (final FireflyId id : vertexPropertyIds.values()) {
                vpProperties.put(id.getStorageId(), new TreeMap<>());
                vpPropertiesTypeHints.put(id.getStorageId(), new TreeMap<>());
            }
            final Bin vpPropertiesBin = new Bin(db.PROPERTIES_BIN, Value.get(vpProperties, MapOrder.KEY_ORDERED));
            final Operation writeVpProperties = Operation.put(vpPropertiesBin);
            final Bin vpPropertiesTypeHintsBin = new Bin(db.TYPE_HINTS_BIN,
                    Value.get(vpPropertiesTypeHints, MapOrder.KEY_ORDERED));
            final Operation writeVpPropertiesTypeHints = Operation.put(vpPropertiesTypeHintsBin);
            final Bin idTypeBin = new Bin(db.ID_TYPE_BIN, Value.get(vertexId.getStorageTypeHint()));
            final Operation writeIdTypeHint = Operation.put(idTypeBin);

            final Key key = getKey(db, db.VERTEX_AERO_SET, vertexId);
            final WritePolicy policy = new WritePolicy();
            if (createOnly) {
                policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            }
            policy.sendKey = true;

            // TODO: This is a temporary measure to pack the user key into a bin. Remove when sendKey works to
            //       recover the user key for hash constructed keys
            if (key.userKey.getObject() != null) {
                final Bin userKeyBin = new Bin(db.USER_KEY_BIN, Value.get(key.userKey.getObject()));
                final Operation writeUserKey = Operation.put(userKeyBin);
                operations.add(writeUserKey);
            }
            operations.add(writeCacheDisabled);
            operations.add(writeLabel);
            operations.add(writeTypeHint);
            operations.add(writeEdgeCacheIn);
            operations.add(writeEdgeCacheOut);
            operations.add(writeVertexPropertyIds);
            operations.add(writeVertexPropertyValues);
            operations.add(writeVertexPropertyTypeHints);
            operations.add(writeVpProperties);
            operations.add(writeVpPropertiesTypeHints);
            operations.add(writeIdTypeHint);

            db.writeOperate(policy, key, operations.toArray(new Operation[0]));
            graph.fireflySummaryUpdater.addVertexWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
            graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, LazyVertexPropertyIdTransform.class);
            final Map<String, LazyIdTransform> lazyIdTransformMap = (Map) vertexPropertyIds;
            final FireflyVertex vertex = FireflyVertexFactory.create(vertexId, label, graph, new TreeMap<>(),
                    new TreeMap<>(), lazyIdTransformMap, vertexPropertyValueMap,
                    vertexPropertyTypeHintMap, vpProperties, vpPropertiesTypeHints,
                    isEdgeCacheOverflowed, db);
            return vertex;
        } else {
            // Should never happen.
            throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }
    }

    /**
     * Get property value map.
     *
     * @param properties Properties.
     * @return Property value map.
     */
    private FireflyVertex.PropertyValueIdMaps getPropertyValueIdMaps(final List<Map.Entry<String, Object>> properties) {
        // Loop through properties nad populate the vertex properties value map.
        final Map<String, Object> vertexPropertyValueMap = new TreeMap<>();
        final Map<String, FireflyId> vertexPropertyIdMap = new TreeMap<>();
        properties.forEach(vp -> {
            // Get id for vertex property.
            final FireflyId vertexPropertyId = graph.getIdFactory().generateId(graph, FireflyVertexProperty.class);

            // Add id and vertex property.
            vertexPropertyValueMap.put(vp.getKey(), vp.getValue());
            vertexPropertyIdMap.put(vp.getKey(), vertexPropertyId);
        });

        // Return vertex property value map.
        return new FireflyVertex.PropertyValueIdMaps(vertexPropertyValueMap, vertexPropertyIdMap);
    }

    /**
     * Get a Vertex directly via ID, bypassing the TTL pushdown filter.
     *
     * @param vertexId ID of Vertex to return
     * @return Vertex
     */
    public FireflyVertex getSingleVertex(final FireflyId vertexId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, vertexId);
        if (fireflyRecord == null) {
            return null;
        }
        final KeyRecord keyRecord = new KeyRecord(fireflyRecord.key(), fireflyRecord.record());
        return FireflyVertexFactory.create(keyRecord, graph);
    }

    public List<FireflyVertex> readVertices(final ReadInfo readInfo) {
        LOG.debug("Reading vertices {}.", readInfo.ids);

        // Batch read vertex records.
        final List<FireflyRecord> vertexRecords = FireflyRecord.batchRead(db, readInfo);
        if (vertexRecords == null) {
            return new ArrayList<>();
        }

        // Convert records to vertices.
        return vertexRecords.stream().
                map(record -> FireflyVertexFactory.create(new KeyRecord(record.key(), record.record()), graph)).
                collect(Collectors.toList());
    }

    public void setCacheDisabled(final FireflyVertex vertex) {
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, vertex.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.db.writeOperate(writePolicy, key,
                Operation.put(new Bin(this.db.EDGE_CACHE_DISABLED_BIN, Value.get(true))));
        vertex.setIsEdgeCacheOverflowed(true);
    }

    public void setTtl(final FireflyVertex vertex, final long durationSeconds) {
        final long expirationTime = System.currentTimeMillis() + (durationSeconds * 1000);
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, vertex.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        final Bin ttlBin = new Bin(this.db.TTL_BIN, expirationTime);
        final Operation writeTtl = Operation.put(ttlBin);
        this.db.writeOperate(writePolicy, key, writeTtl);
    }

    public Iterator<KeyRecord> getEdgeKeyRecordsByIndex(final FireflyVertex vertex,
                                                        final Direction direction,
                                                        final Set<String> labels,
                                                        final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                        final List<HasContainer> hasContainers,
                                                        final FireflyId adjacentVertexId) {
        if (outputType == FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID && !hasContainers.isEmpty()) {
            // This should never happen.
            throw new IllegalStateException("Pushdown filters are not supported for composite ID edge skipping.");
        }
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.sendKey = true;
        queryPolicy.includeBinData = true;
        queryPolicy.filterExp = GraphQueryHelper.phatEdgeHasContainerListToExpression(db, hasContainers, labels,
                vertex.id.getKeyHashString(), adjacentVertexId, direction);
        if (direction == Direction.OUT) {
            return new CachedIterator(graph, graph.graphQuery.querySIndex(db.EDGE_AERO_SET, db.E_OUT_INDEX_NAME,
                    Filter.contains(db.SUPERNODES_OUT_BIN, IndexCollectionType.MAPVALUES, vertex.id.getKeyHashString()),
                    queryPolicy));
        } else if (direction == Direction.IN) {
            return new CachedIterator(graph, graph.graphQuery.querySIndex(db.EDGE_AERO_SET, db.E_IN_INDEX_NAME,
                    Filter.contains(db.SUPERNODES_IN_BIN, IndexCollectionType.MAPVALUES, vertex.id.getKeyHashString()),
                    queryPolicy));
        } else {
            // This should never happen since this method is not invoked with BOTH.
            throw new RuntimeException("Can not query adjacency index with Direction BOTH.");
        }
    }

    // Tag supernode reads in cache.
    private static class CachedIterator implements Iterator<KeyRecord> {
        final Iterator<KeyRecord> keyRecordIterator;
        final FireflyCache cache;

        CachedIterator(final FireflyGraph graph, final Iterator<KeyRecord> keyRecordIterator) {
            this.keyRecordIterator = keyRecordIterator;
            this.cache = graph.getBaseGraph().transactionCache.get();
        }

        @Override
        public boolean hasNext() {
            return keyRecordIterator.hasNext();
        }

        @Override
        public KeyRecord next() {
            final KeyRecord keyRecord = keyRecordIterator.next();
            if (cache != null) {
                cache.insert(keyRecord.key, keyRecord.record);
            }
            return keyRecord;
        }
    }

    /**
     * Remove vertex from Aerospike.
     */
    public void removeVertex(final FireflyVertex vertex) {
        final Txn txn = getOrCreateTxn();

        try {
            // Collect edges in both directions and remove them all.
            final Iterator<FireflyEdge> inEdges = new FireflyBatchEdgeIterator(graph, vertex.getEdgeIdsFromVertex(Direction.IN, Collections.emptySet(), Collections.emptyList()));
            final Iterator<FireflyEdge> outEdges = new FireflyBatchEdgeIterator(graph, vertex.getEdgeIdsFromVertex(Direction.OUT, Collections.emptySet(), Collections.emptyList()));
            // Remove the edges themselves and from the adjacent vertices' edge caches.
            inEdges.forEachRemaining(e -> removeEdge(e, true, false, txn));
            outEdges.forEachRemaining(e -> removeEdge(e, false, true, txn));

            // Remove vertex.
            LOG.debug("Removing vertex {}.", vertex.id);
            if (db.delete(FireflyRecord.getKey(db, db.VERTEX_AERO_SET, vertex.id), txn)) {
                graph.fireflySummaryUpdater.addVertexRemoveToQueue(vertex.label());
            }

            db.commit(txn);

            if (graph.getBaseGraph().IS_AUDIT_LOG_ENABLED) {
                LOG.info("[{}] Dropped vertex with id: {}", graph.getUser(), vertex.id());
            }
        } catch (final RuntimeException e) {
            db.rollback(txn);
            throw e;
        }
    }

    //////////////// VERTEX PROPERTIES ///////////////

    /**
     * Write properties to element.
     *
     * @param propertyKey   Key of property to write.
     * @param propertyValue Value of property to write.
     * @param <F>           Type of property value.
     * @return Map of label to properties.
     */
    public <F> Property<F> writeVertexProperty(final FireflyVertexProperty vertexProperty, final String propertyKey, final F propertyValue) {
        final Key opKey = getKey(db, db.VERTEX_AERO_SET, vertexProperty.vertexId);
        final List<Operation> operations = new ArrayList<>();

        final Operation writeValue;
        if (propertyValue == null) {
            writeValue = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(Value.get(vertexProperty.id.getStorageId())));
            operations.add(writeValue);
            final Operation writeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(propertyKey), MapReturnType.NONE,
                    CTX.mapKey(Value.get(vertexProperty.id.getStorageId())));
            operations.add(writeTypeHint);
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            writeValue = MapOperation.put(policy, db.PROPERTIES_BIN, Value.get(propertyKey), Value.get(propertyValue),
                    CTX.mapKey(Value.get(vertexProperty.id.getStorageId())));
            operations.add(writeValue);
            final Object typeHint = Value.get(getTypeHintOf(propertyValue));
            if (typeHint != null) {
                final Operation writeTypeHint = MapOperation.put(policy, db.TYPE_HINTS_BIN, Value.get(propertyKey),
                        Value.get(typeHint),
                        CTX.mapKey(Value.get(vertexProperty.id.getStorageId())));
                operations.add(writeTypeHint);
            }
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, opKey, operations.toArray(new Operation[0]));
        } catch (final AerospikeGraphRecordSizeExceededException rtbe) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVpProperty((AerospikeException) rtbe.getCause(), db, getRelevantVertexBins(db, opKey),
                            vertexProperty.vertexId, vertexProperty.key(), propertyKey);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Vertex Property has been removed from the Vertex since in this case
                // the key is the Vertex key due to Vertex Properties being packed and thus the key still exists.
                LOG.error("Vertex Property with ID {} no longer exists.", vertexProperty.id());
                throw new AerospikeGraphElementNotFoundException();
            } else {
                throw ae;
            }
        }
        vertexProperty.properties.put(propertyKey, propertyValue);
        if (propertyValue == null || getTypeHintOf(propertyValue) == null) {
            vertexProperty.typeHints.remove(propertyKey);
        } else {
            vertexProperty.typeHints.put(propertyKey, getTypeHintOf(propertyValue));
        }
        return new FireflyVertexPropertyProperty<>(graph, vertexProperty, propertyKey, propertyValue);
    }

    /**
     * Write vertex property to vertex.
     *
     * @param vertexProperty Vertex property to write to vertex.
     */
    public void writeVertexProperty(final FireflyVertex vertex, final FireflyVertexProperty vertexProperty) {
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, vertex.id);
        final List<Operation> operations = new ArrayList<>();
        boolean wroteTypeHint = false;

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation putValue = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                Value.get(vertexProperty.key()), Value.get(vertexProperty.value()));
        operations.add(putValue);
        final Object typeHint = getTypeHintOf(vertexProperty.value());
        if (typeHint != null) {
            final Operation putTypeHint = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN,
                    Value.get(vertexProperty.key()), Value.get(typeHint));
            operations.add(putTypeHint);
            wroteTypeHint = true;
        }
        final Operation putId = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN,
                Value.get(vertexProperty.key()), Value.get(vertexProperty.id.getStorageId()));
        operations.add(putId);
        final Operation getValues = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
        operations.add(getValues);
        final Operation getTypeHints = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
        operations.add(getTypeHints);
        final Operation getIds = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN);
        operations.add(getIds);

        // Write key for the vertex property's properties
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation addKeyProperties = MapOperation.put(mapPolicy, this.db.PROPERTIES_BIN,
                Value.get(vertexProperty.id.getStorageId()), Value.get(vertexProperty.properties));
        operations.add(addKeyProperties);
        final Operation addKeyPropertiesTypeHints = MapOperation.put(mapPolicy, this.db.TYPE_HINTS_BIN,
                Value.get(vertexProperty.id.getStorageId()), Value.get(vertexProperty.typeHints));
        operations.add(addKeyPropertiesTypeHints);
        final Operation getKeyProperties = Operation.get(this.db.PROPERTIES_BIN);
        operations.add(getKeyProperties);
        final Operation getKeyPropertiesTypeHints = Operation.get(this.db.TYPE_HINTS_BIN);
        operations.add(getKeyPropertiesTypeHints);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record result = this.db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));

            final Map<String, Object> vertexPropertyValues = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN, 1)).orElse(new TreeMap<>());
            final Map<String, Object> vertexPropertyTypeHints = wroteTypeHint ?
                    (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN, 1)).orElse(new TreeMap<>()) :
                    (Map<String, Object>) result.getMap(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
            final Map<String, Object> vertexPropertyIds = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN, 1)).orElse(new TreeMap<>());
            final Map<Object, Map<String, Object>> vertexPropertyIdToProperties = (Map<Object, Map<String, Object>>) Optional.ofNullable(getValueAtIndex(result, this.db.PROPERTIES_BIN, 1)).orElse(new TreeMap<>());
            final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints = (Map<Object, Map<String, Object>>) Optional.ofNullable(getValueAtIndex(result, this.db.TYPE_HINTS_BIN, 1)).orElse(new TreeMap<>());
            this.graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, LazyVertexPropertyIdTransform.class);
            final Map<String, LazyIdTransform> vertexPropertyFireflyIds = (Map) vertexPropertyIds;

            // Update this FireflyVertex in JVM cache
            vertex.updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
            graph.fireflySummaryUpdater.addVertexPropertiesWriteToQueue(vertex.label(), Set.of(vertexProperty.key()));
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVertexProperty((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, key), vertex.id, vertexProperty.key());
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    /**
     * Remove the vertex property from the vertex.
     *
     * @param key              vertex property label.
     * @param vertexPropertyId id of the vertex property to remove.
     */
    public void removeVertexProperty(final FireflyVertex vertex, final String key, final FireflyId vertexPropertyId) {
        final Key opKey = getKey(this.db, this.db.VERTEX_AERO_SET, vertex.id);

        // Remove Vertex Property's Properties.
        final Operation removeProperty =
                MapOperation.removeByKey(this.db.PROPERTIES_BIN, Value.get(vertexPropertyId.getStorageId()), MapReturnType.NONE);
        final Operation removePropertyTypeHint =
                MapOperation.removeByKey(this.db.TYPE_HINTS_BIN, Value.get(vertexPropertyId.getStorageId()), MapReturnType.NONE);

        // Remove Vertex Property.
        final Operation removeVertexPropertyValue =
                MapOperation.removeByKey(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN, Value.get(key), MapReturnType.NONE);
        final Operation removeVertexPropertyTypeHint =
                MapOperation.removeByKey(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN, Value.get(key), MapReturnType.NONE);
        final Operation removeVertexPropertyId =
                MapOperation.removeByKey(this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN, Value.get(key), MapReturnType.NONE);

        final Operation getVertexPropertyValues = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
        final Operation getVertexPropertyValuesTypeHints =
                Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
        final Operation getVertexPropertyIds = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN);
        final Operation getVertexPropertyProperties = Operation.get(this.db.PROPERTIES_BIN);
        final Operation getVertexPropertyTypeHints = Operation.get(this.db.TYPE_HINTS_BIN);

        final FireflyCache cache = this.db.transactionCache.get();
        final FireflyCache noPropsCache = this.db.emptyPropsTransactionCache.get();
        if (cache != null) {
            cache.invalidate(opKey);
        }
        if (noPropsCache != null) {
            noPropsCache.invalidate(opKey);
        }

        final Record result = this.db.writeOperate(null, opKey, removeProperty, removePropertyTypeHint,
                removeVertexPropertyValue, removeVertexPropertyId, removeVertexPropertyTypeHint,
                getVertexPropertyValues, getVertexPropertyValuesTypeHints, getVertexPropertyIds,
                getVertexPropertyProperties, getVertexPropertyTypeHints);

        final Map<String, Object> vertexPropertyValues =
                (Map<String, Object>) getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN, 1);
        final Map<String, Object> vertexPropertyValuesTypeHints =
                (Map<String, Object>) getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN, 1);
        final Map<String, Object> vertexPropertyIds =
                (Map<String, Object>) getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN, 1);
        final Map<Object, Map<String, Object>> vertexPropertyIdToProperties =
                (Map<Object, Map<String, Object>>) getValueAtIndex(result, this.db.PROPERTIES_BIN, 1);
        final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints =
                (Map<Object, Map<String, Object>>) getValueAtIndex(result, this.db.TYPE_HINTS_BIN, 1);
        this.graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, LazyVertexPropertyIdTransform.class);
        final Map<String, LazyIdTransform> vertexPropertyFireflyIds = (Map) vertexPropertyIds;


        // Update this FireflyVertex in JVM cache
        vertex.updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues,
                vertexPropertyValuesTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
    }

    //////////////// EDGE OPERATIONS ///////////////

    /**
     * Write edge to record. This does not write to edge caches on attached vertices.
     *
     * @param edgeId              Id of Edge to write.
     * @param label               label for Edge to write.
     * @param inVertex            in Vertex for new Edge.
     * @param outVertex           out Vertex for new Edge.
     * @param inVertexCacheWrite  was the edge written to the edge cache of the in vertex.
     * @param outVertexCacheWrite was the edge written to the edge cache of the out vertex.
     * @param properties          Edge properties.
     */
    public FireflyEdge writeEdgeToRecord(final FireflyId edgeId,
                                         final String label,
                                         final List<Map.Entry<String, Object>> properties,
                                         final FireflyVertex inVertex,
                                         final FireflyVertex outVertex,
                                         final boolean inVertexCacheWrite,
                                         final boolean outVertexCacheWrite,
                                         final Txn txn) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertex.id(), label, inVertex.id(), properties);

        final Map<String, Object> propertyMap = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        properties.forEach(property -> {
            final String key = property.getKey();
            final Object value = FireflyHelper.validatePropertyValue(property.getValue());

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
        final Value edgeIdkey = Value.get(((FireflyEdgeId) edgeId).getEdgeIdBytes());
        if (!inVertexCacheWrite) {
            final Operation writeInVSupernode = MapOperation.put(edgeMapPolicy, db.SUPERNODES_IN_BIN,
                    edgeIdkey, Value.get(inVertex.id.getKeyHashString()));
            operations.add(writeInVSupernode);
        }
        if (!outVertexCacheWrite) {
            final Operation writeOutVSupernode = MapOperation.put(edgeMapPolicy, db.SUPERNODES_OUT_BIN,
                    edgeIdkey, Value.get(outVertex.id.getKeyHashString()));
            operations.add(writeOutVSupernode);
        }

        // Handle TTL.
        long ttlValueLong;
        if (propertyMap.containsKey(TTL_PROPERTY_KEY)) {
            if (!db.TTL_ENABLED_FLAG) {
                throw new AerospikeGraphException(GraphError.TTL_NOT_ENABLED);
            }
            final Object ttlValue = propertyMap.remove(TTL_PROPERTY_KEY);
            typeHints.remove(TTL_PROPERTY_KEY);
            if (Number.class.isAssignableFrom(ttlValue.getClass())) {
                ttlValueLong = ((Number) ttlValue).longValue();
                final long expirationTime = System.currentTimeMillis() + (ttlValueLong * 1000);
                final Operation writeTtl = MapOperation.put(edgeMapPolicy, db.TTL_BIN, edgeIdkey,
                        Value.get(expirationTime));
                operations.add(writeTtl);
            } else {
                throw new IllegalArgumentException(
                        String.format("Property value [%s] for key %s is of type %s and must be numeric", ttlValue,
                                TTL_PROPERTY_KEY, ttlValue.getClass()));
            }
        }

        // Write to filterable supernode bin if necessary.
        operations.addAll(createFilterableSupernodeOperations((FireflyPhatEdgeId) edgeId, !outVertexCacheWrite,
                !inVertexCacheWrite, outVertex.id, inVertex.id, label, propertyMap));

        // Add properties and type hints to Edge data.
        edgeData.add(PROPERTIES_POSITION, Value.get(propertyMap));
        edgeData.add(TYPE_HINTS_POSITION, Value.get(typeHints));

        // Create Operation for writing Edge data.
        final Operation createIndividualEdgeMap = MapOperation.put(edgeMapPolicy, db.EDGE_DATA_BIN,
                edgeIdkey, Value.get(edgeData));
        operations.add(createIndividualEdgeMap);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.txn = txn;
        writePolicy.sendKey = true;
        final Key key = getKey(db, db.EDGE_AERO_SET, edgeId);
        try {
            final Record record = db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));
            graph.fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
            final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id,
                    propertyMap, typeHints, !outVertexCacheWrite, !inVertexCacheWrite, record.generation);
            return edge;
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingEdge((AerospikeException) e.getCause(), db, key, edgeId);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    /**
     * Function to write edge to Aerospike.
     *
     * @param edgeId     Edge id.
     * @param label      Edge label.
     * @param properties Edge properties.
     * @param inVertex   In vertex of edge.
     * @param outVertex  Out vertex of edge.
     * @return Edge.
     */
    public FireflyEdge writeEdge(final FireflyEdgeId edgeId,
                                 final String label,
                                 final List<Map.Entry<String, Object>> properties,
                                 final FireflyVertex inVertex,
                                 final FireflyVertex outVertex) {
        final Txn txn = getOrCreateTxn();

        try {
            // Write edge to vertex, if edge write fails, null check on edge record will protect from inconsistent data.
            // Add edge to inVertex and outVertex.
            final boolean inVertexCacheWrite = writeEdgeToVertex(inVertex, Direction.IN, graph.getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label, txn);
            final boolean outVertexCacheWrite = writeEdgeToVertex(outVertex, Direction.OUT, graph.getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label, txn);

            // Write edge to Aerospike and return FireflyEdge.
            final FireflyEdge edge = writeEdgeToRecord(edgeId, label, properties, inVertex, outVertex, inVertexCacheWrite, outVertexCacheWrite, txn);

            db.commit(txn);

            if (db.IS_AUDIT_LOG_ENABLED) {
                // Edge id is byte buffer so not useful.
                LOG.info("[{}] created edge: [{}]-[{}]>[{}].", graph.getUser(), inVertex.id(), label, outVertex.id());
            }
            return edge;
        } catch (final RuntimeException e) {
            db.rollback(txn);
            throw e;
        }
    }

    /**
     * Write edge to vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     * @return was the edge written to this vertex's edge cache.
     */
    public boolean writeEdgeToVertex(final FireflyVertex vertex, final Direction direction, final FireflyId edgeId,
                                     final String edgeLabel, final Txn txn) {
        LOG.debug("Writing Edge {} to ECACHE of Vertex {} with Direction {}.", edgeId, this, direction);
        // Edge cache is overflowed for this vertex - do nothing since writing to overflow bin is on the edge record.
        if (!vertex.writeEdge(direction, edgeId, edgeLabel)) {
            return false;
        }

        // Get bin name for edge direction.
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES_BIN : db.OUT_EDGES_BIN;

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, vertex.id);

        // Create operations for writing to edge cache.
        final ListPolicy preventDuplicates = new ListPolicy(ListOrder.UNORDERED,
                ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);
        final Operation appendToEdgeCache = ListOperation.append(
                preventDuplicates,
                cacheBinName,
                Value.get(edgeId.getCachedId()),
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );
        final Expression cacheState = Exp.build(
                Exp.cond(
                        // Set the cache disabled flag to true if it was already true or the counter now exceeds the
                        // cache limit size. Need the OR check to prevent the cache from being re-enabled if concurrent
                        // traversals removed edges and reduced the edge counter.
                        Exp.or(
                                Exp.boolBin(this.db.EDGE_CACHE_DISABLED_BIN),
                                Exp.ge(Exp.val(vertex.getEdgeCount(direction)), Exp.val(this.db.ON_RECORD_ID_LIMIT))
                        ),
                        Exp.val(true),
                        Exp.val(false)
                )
        );
        final Operation updateCacheState = ExpOperation.write(this.db.EDGE_CACHE_DISABLED_BIN, cacheState, ExpWriteFlags.DEFAULT);
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED_BIN);

        // Operate on database.
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.txn = txn;
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record results = this.db.writeOperate(writePolicy, key, appendToEdgeCache, updateCacheState, getCacheDisabled);

            vertex.setIsEdgeCacheOverflowed(
                    (boolean) OperationReturnHandler.getValueAtIndex(results, this.db.EDGE_CACHE_DISABLED_BIN, 1));
            return true;
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingToEdgeCache((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, key), vertex.id, edgeId);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    // used in bulk loader
    public void writeBadEdge(final Object badVertexId, final long count) {
        final FireflyId id = graph.getIdFactory().createVertexId(badVertexId);
        final Key key = new Key(db.namespace, db.BULK_LOAD_BAD_EDGE_SET, Value.get(id.getStorageId()));
        final Bin addBin = new Bin(db.COUNTER_BIN, count);
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        policy.sendKey = true;
        try {
            this.db.writeOperate(policy, key, Operation.add(addBin));
        } catch (final AerospikeGraphException e) {
            // Do not retry this or fail because this list being slightly incorrect is inconsequential and will reduce
            // bulk load speed
            LOG.warn("Error recording bad Edge data with failed Vertex ID: {}", badVertexId);
        }
    }

    public List<Operation> createFilterableSupernodeOperations(final FireflyPhatEdgeId edgeId,
                                                               final boolean isOutSupernode, final boolean isInSupernode,
                                                               final FireflyId outVId, final FireflyId inVId,
                                                               final String label, final Map<String, Object> propertyMap) {
        // This method is concurrent traversal safe for removed edges since it's only invoked on creation of a new edge.
        if (!db.isSupernodePushdownEnabled || (!isOutSupernode && !isInSupernode)) {
            // No supernodes so we don't have to do anything
            return Collections.emptyList();
        }

        final String binName = db.SUPERNODE_EDGE_PROPERTIES_BIN;
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
            if (db.isMergeEdgeDataModelEnabled) {
                final Operation adjacentVOperation = MapOperation.put(policy, binName, edgeUniqueId, inVIdValue,
                        CTX.mapKeyCreate(outVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_IN_KEY), MapOrder.KEY_ORDERED));
                operations.add(adjacentVOperation);
            }
        }
        if (isInSupernode) {
            final Operation labelOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(label),
                    CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_LABEL_KEY), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
            if (db.isMergeEdgeDataModelEnabled) {
                final Operation adjacentVOperation = MapOperation.put(policy, binName, edgeUniqueId, outVIdValue,
                        CTX.mapKeyCreate(inVIdValue, MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(EDGE_SUPERNODE_OUT_KEY), MapOrder.KEY_ORDERED));
                operations.add(adjacentVOperation);
            }
        }
        // Properties
        for (final Map.Entry<String, Object> property : propertyMap.entrySet()) {
            appendFilterableSupernodePropertyOperation(edgeId, isOutSupernode, isInSupernode, outVId, inVId,
                    property.getKey(), property.getValue(), operations);
        }
        return operations;
    }

    public void appendFilterableSupernodePropertyOperation(final FireflyPhatEdgeId edgeId,
                                                           final boolean isOutSupernode, final boolean isInSupernode,
                                                           final FireflyId outVId, final FireflyId inVId,
                                                           final String propertyKey, final Object propertyValue,
                                                           final List<Operation> operations) {
        // This method is concurrent traversal safe for removed edges since this should only be invoked with conjunction
        // of adding a property normally, which have operations that fail if the edge was already removed.
        if (!db.isSupernodePushdownEnabled || (!isOutSupernode && !isInSupernode)) {
            // No supernodes so we don't have to do anything
            return;
        }

        final String binName = db.SUPERNODE_EDGE_PROPERTIES_BIN;
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

    private static boolean isPropertyValuePushdownable(final Object propertyValue) {
        final Class<?> propertyValueClass = propertyValue.getClass();
        return String.class.isAssignableFrom(propertyValueClass) ||
                Long.class.isAssignableFrom(propertyValueClass) ||
                Integer.class.isAssignableFrom(propertyValueClass);
    }

    private void appendRemoveFilterableSupernodePropertyOperation(final FireflyEdge fireflyEdge,
                                                                  final String propertyKey,
                                                                  final List<Operation> operations) {
        final boolean isOutSupernode = fireflyEdge.isOutSupernode();
        final boolean isInSupernode = fireflyEdge.isInSupernode();
        final FireflyPhatEdgeId edgeId = (FireflyPhatEdgeId) fireflyEdge.id;

        if (!db.isSupernodePushdownEnabled || (!isOutSupernode && !isInSupernode)) {
            // No supernodes so we don't have to do anything
            return;
        }

        final String binName = db.SUPERNODE_EDGE_PROPERTIES_BIN;
        final Exp edgeUniqueId = Exp.val(edgeId.getUniqueId());
        final Value propertyKeyValue = Value.get(propertyKey);
        final Exp propertyKeyExp = Exp.val(propertyKey);
        final int writeFlags = ExpWriteFlags.EVAL_NO_FAIL;

        if (isOutSupernode) {
            // Remove Edge ID : Property Value
            final Value outVIdValue = Value.get(fireflyEdge.outVertexId().getKeyHashString());
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
            final Value inVIdValue = Value.get(fireflyEdge.inVertexId().getKeyHashString());
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

    public void setEdgeTTL(final FireflyEdge edge, final long durationSeconds) {
        final Key key = getKey(db, this.db.EDGE_AERO_SET, edge.id);
        final long expirationTime = System.currentTimeMillis() + (durationSeconds * 1000);
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation writeTtl = MapOperation.put(policy, db.TTL_BIN, edgeIdMapKey, Value.get(expirationTime));

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, key, writeTtl);
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingProperty((AerospikeException) e.getCause(), db, key, edge.id, TTL_PROPERTY_KEY);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case
                // the key is the Phat Edge key and thus the key still exists.
                LOG.error("Edge with ID {} no longer exists.", edge.id());
                throw new AerospikeGraphElementNotFoundException();
            } else {
                throw ae;
            }
        }
    }

    /**
     * Remove edge from Aerospike.
     * <p>
     * Note: This does not remove the edge from its attached vertices.
     */
    public void removeEdge(final FireflyEdge edge) {
        removeEdge(edge, false, false, null);
    }

    /**
     * Remove edge from Aerospike.
     */
    public void removeEdge(final FireflyEdge edge, final boolean fromOutV, final boolean fromInV, final Txn outerTxn) {
        final Txn txn = outerTxn == null ? getOrCreateTxn() : outerTxn;

        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final List<Operation> operations = new ArrayList<>();
        final Value idKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());

        final Operation removeEdgeData = MapOperation.removeByKey(db.EDGE_DATA_BIN, idKey, MapReturnType.VALUE);
        operations.add(removeEdgeData);
        final Operation removeSupernodesIn = MapOperation.removeByKey(db.SUPERNODES_IN_BIN, idKey, MapReturnType.NONE);
        operations.add(removeSupernodesIn);
        final Operation removeSupernodesOut = MapOperation.removeByKey(db.SUPERNODES_OUT_BIN, idKey, MapReturnType.NONE);
        operations.add(removeSupernodesOut);
        if (db.isSupernodePushdownEnabled) {
            edge.properties().forEachRemaining(p -> {
                if (isPropertyValuePushdownable(p.value())) {
                    appendRemoveFilterableSupernodePropertyOperation(edge, p.key(), operations);
                }
            });

            // Remove adjacent Vertex ID and label pushdowns
            final Exp edgeUniqueId = Exp.val(((FireflyPhatEdgeId) edge.id).getUniqueId());
            final Exp supernodePBinExp = Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN);
            final CTX labelMapKeyCtx = CTX.mapKey(Value.get(EDGE_SUPERNODE_LABEL_KEY));
            final int writeFlags = ExpWriteFlags.EVAL_NO_FAIL;
            if (edge.isOutSupernode()) {
                final Value outVIdValue = Value.get(edge.outVertexId().getKeyHashString());
                final Exp outVIdExp = Exp.val(edge.outVertexId().getKeyHashString());
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
            if (edge.isInSupernode()) {
                final Value inVidValue = Value.get(edge.inVertexId().getKeyHashString());
                final Exp inVidExp = Exp.val(edge.inVertexId().getKeyHashString());
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
        final Operation removeTtl = MapOperation.removeByKey(db.TTL_BIN, idKey, MapReturnType.NONE);
        operations.add(removeTtl);

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
        final Operation removeTtlBin = ExpOperation.write(db.TTL_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeTtlBin);

        // This operation must be last since the expression checks the map in the edge data bin.
        final Operation removeEdgeDataBin = ExpOperation.write(db.EDGE_DATA_BIN, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeEdgeDataBin);

        final WritePolicy policy = new WritePolicy();
        policy.txn = txn;
        policy.durableDelete = txn != null;
        if (db.isSupernodePushdownEnabled && (edge.isInSupernode() || edge.isOutSupernode())) {
            policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            policy.generation = edge.getGeneration();
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
                    graph.getIdFactory().recycleEdgeId(edge.id);
                    final String label = (String) ((List<?>) edgeData).get(LABEL_POSITION);
                    graph.fireflySummaryUpdater.addEdgeRemoveToQueue(label);
                } else if (edgeData != null) {
                    // This should never happen.
                    throw new RuntimeException("Individual Edge data in Phat Edge returned as type that is of type: " + edgeData.getClass());
                }
                LOG.debug("Ignoring exception when deleting Edge with id {} since it was not found.", edge.id.getUserId());
            }
        } catch (final AerospikeGraphElementNotFoundException e) {
            // This tends to occur when deleting multiple vertices in a single traversal where the Edge lives in between
            // the to-be-deleted vertices.
            LOG.debug("Ignoring exception when deleting Edge with id {} since it was not found.", edge.id.getUserId());
        } catch (final AerospikeGraphException e) {
            if (e.errorCode != ResultCode.GENERATION_ERROR) {
                // rollback only own txn
                if (outerTxn == null)
                    db.rollback(txn);
                throw e;
            }
            LOG.debug("Regenerating supernode Edge with id {} to clean up supernode property bin.", edge.id.getUserId());
            final List<FireflyEdge> edges = readEdges(List.of(edge.id));
            // If no edges come back that's okay, because it means a different traversal has cleaned up this edge.
            if (!edges.isEmpty()) {
                if (edges.size() > 1) {
                    // This should never happen.
                    final String message = "Regeneration of supernode edge during delete returned multiple edges for a single id: " + edge.id.getUserId();
                    LOG.error(message);
                    throw new IllegalStateException(message);
                }
                removeEdge(edges.get(0), false, false, txn);
            }
            LOG.debug("Generation check retry failed when regenerating Edge with id {} since it was not found.", edge.id.getUserId());
        }

        try {
            if (fromOutV) {
                removeEdgeFromOut(edge, txn);
            }

            if (fromInV) {
                removeEdgeFromIn(edge, txn);
            }
            edge.removed = true;

            // commit only own txn
            if (outerTxn == null)
                db.commit(txn);
        } catch (final AerospikeGraphException e) {
            if (outerTxn == null)
                db.rollback(txn);
            throw e;
        }

        if (this.db.IS_AUDIT_LOG_ENABLED) {
            LOG.info("[{}] Dropped edge [{}]-[{}]>[{}].", graph.getUser(), edge.outVertex().id(), edge.label(), edge.inVertex().id());
        }
    }

    private void removeEdgeFromOut(final FireflyEdge edge, final Txn txn) {
        // If OUT is a supernode it means this Edge does not exist on its record so skip reading it.
        if (!edge.isOutSupernode()) {
            final FireflyVertex outVertex = this.graph.readVertex(edge.outVertexId());
            if (outVertex != null) {
                removeEdgeFromVertex(outVertex, Direction.OUT, (FireflyPhatEdgeId) edge.id, edge.label(), txn);
            }
        }
    }

    private void removeEdgeFromIn(final FireflyEdge edge, final Txn txn) {
        // If IN is a supernode it means this Edge does not exist on its record so skip reading it.
        if (!edge.isInSupernode()) {
            final FireflyVertex inVertex = this.graph.readVertex(edge.inVertexId());
            if (inVertex != null) {
                removeEdgeFromVertex(inVertex, Direction.IN, (FireflyPhatEdgeId) edge.id, edge.label(), txn);
            }
        }
    }

    /**
     * Remove edge from vertex.
     *
     * @param vertex    Vertex to remove from
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    private void removeEdgeFromVertex(final FireflyVertex vertex, final Direction direction,
                                      final FireflyPhatEdgeId edgeId, final String edgeLabel, final Txn txn) {
        final FireflyId compositeIdToRemove = vertex.removeEdge(direction, edgeId, edgeLabel);
        // wrong id or already removed?
        if (compositeIdToRemove == null)
            return;

        // Get bin name for edge direction.
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES_BIN : db.OUT_EDGES_BIN;

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, vertex.id);

        // Create operations for removing from edge cache.
        final Operation removeEdgeId = ListOperation.removeByValue(
                cacheBinName,
                Value.get(compositeIdToRemove.getCachedId()),
                ListReturnType.NONE,
                CTX.mapKey(Value.get(edgeLabel))
        );
        final Expression removeEmptyKey = Exp.build(
                Exp.cond(
                        Exp.eq(ListExp.size(Exp.mapBin(cacheBinName), CTX.mapKey(Value.get(edgeLabel))), Exp.val(0)),
                        MapExp.removeByKey(Exp.val(edgeLabel), Exp.mapBin(cacheBinName)),
                        Exp.unknown()
                )
        );
        final Operation removeEmptyEdgeCacheKeys = ExpOperation.write(cacheBinName, removeEmptyKey, ExpWriteFlags.EVAL_NO_FAIL);

        // Removing an edge can never change the state of the edge cache so only need to read in case of cache disabling
        // due to concurrent traversals.
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED_BIN);

        // Operate on database.
        try {
            WritePolicy policy = new WritePolicy();
            policy.txn = txn;
            final Record results =
                    this.db.writeOperate(policy, key, removeEdgeId, removeEmptyEdgeCacheKeys, getCacheDisabled);

            vertex.setIsEdgeCacheOverflowed(results.getBoolean(this.db.EDGE_CACHE_DISABLED_BIN));
        } catch (final AerospikeGraphElementNotFoundException enfe) {
            // This Vertex's record was deleted concurrently and thus the record does not exist.
            LOG.debug("Error removing edge id {} from edge cache of vertex {}; the vertex was deleted.",
                    edgeId.getUserId(), vertex.id.getUserId());
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when concurrent traversals remove the same Edge ID from the ECACHE and the
                // Edge is the last of its Label category, meaning the later traversal will fail due to an operation
                // working under the assumption that the Label exists.
                LOG.warn("Error removing edge id {} from edge cache of vertex {}; this is likely from a concurrent removal.",
                        edgeId.getUserId(), vertex.id.getUserId());
            } else {
                throw ae;
            }
        }
    }

    /**
     * Read Edges with the provided list of IDs.
     *
     * @param edgeIds Edge ids to read.
     * @return Edge.
     */
    public List<FireflyEdge> readEdges(final List<FireflyId> edgeIds) {
        final List<FireflyEdge> edges = new ArrayList<>();
        final Map<FireflyId, FireflyRecord> edgeRecords = FireflyRecord.batchReadPhatEdges(db, edgeIds);
        if (edgeRecords.isEmpty()) {
            return edges;
        }

        for (final FireflyId edgeId : edgeIds) {
            final FireflyRecord edgeRecord = edgeRecords.get(edgeId);
            if (edgeRecord != null) {
                final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, edgeRecord.record(), graph);
                if (edge != null) {
                    edges.add(edge);
                }
            }
        }

        return edges;
    }

    //////////////// EDGE PROPERTIES ///////////////

    public <V> Property<V> writeProperty(final FireflyEdge edge, final String propertyKey, final V value) {
        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());

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

        appendFilterableSupernodePropertyOperation((FireflyPhatEdgeId) edge.id, edge.isOutSupernode(),
                edge.isInSupernode(), edge.outVertexId(), edge.inVertexId(), propertyKey, value, operations);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));
            edge.addPropertyToCache(propertyKey, value);
            if (typeHint != null) {
                edge.addTypeHint(propertyKey, typeHint);
            }
            graph.fireflySummaryUpdater.addEdgePropertiesWriteToQueue(edge.label(), Set.of(propertyKey));
            return new FireflyEdgeProperty<>(graph, edge, propertyKey, value);
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingProperty((AerospikeException) e.getCause(), db, key, edge.id, propertyKey);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case
                // the key is the Phat Edge key and thus the key still exists.
                LOG.error("Edge with ID {} no longer exists.", edge.id());
                throw new AerospikeGraphElementNotFoundException();
            } else {
                throw ae;
            }
        }
    }

    public void removeEdgeProperty(final FireflyEdgeProperty property) {
        final Key key = getKey(db, db.EDGE_AERO_SET, property.edge().id);
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) property.edge().id).getEdgeIdBytes());
        final List<Operation> operations = new ArrayList<>();

        final Operation removeProperty = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(property.key()),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
        operations.add(removeProperty);
        final Operation removeTypeHint = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(property.key()),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
        operations.add(removeTypeHint);
        try {
            if (isPropertyValuePushdownable(property.value())) {
                appendRemoveFilterableSupernodePropertyOperation(property.edge(), property.key(), operations);
            }
        } catch (final NoSuchElementException e) {
            // Do nothing since a property with no value is the same as a value type that can't be pushed down.
        }

        try {
            property.edge().removePropertyFromCache(property.key());
            db.writeOperate(null, key, operations.toArray(new Operation[0]));
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case the key is
                // the Phat Edge key and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed property {}", this, ae);
            } else {
                throw ae;
            }
        }
    }
}
