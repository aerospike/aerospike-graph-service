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
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.io.aerospike.query.paged.EdgeQueryHelper;
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
import com.aerospike.firefly.structure.iterator.FireflyBatchEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphElementNotFoundException;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.AerospikeGraphRecordSizeExceededException;
import com.aerospike.firefly.util.exceptions.EdgeRecordSizeExceededException;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.aerospike.firefly.util.exceptions.TtlArgumentException;
import com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.io.aerospike.FireflyTxn.commit;
import static com.aerospike.firefly.io.aerospike.FireflyTxn.rollback;
import static com.aerospike.firefly.io.aerospike.OperationReturnHandler.getValueAtIndex;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_DATA_SIZE;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_ADJACENT_ID_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;
import static com.aerospike.firefly.structure.FireflyElement.TTL_PROPERTY_KEY;
import static com.aerospike.firefly.util.FireflyHelper.validateAndConvertVertexPropertyValue;
import static com.aerospike.firefly.util.exceptions.EdgeRecordSizeExceededException.fromAddingEdge;
import static com.aerospike.firefly.util.exceptions.EdgeRecordSizeExceededException.fromAddingProperty;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.fromAddingToEdgeCache;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.fromAddingVertexProperty;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.fromAddingVpProperty;
import static com.aerospike.firefly.util.exceptions.VertexRecordSizeExceededException.getRelevantVertexBins;

public class AerospikeOperations {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeOperations.class);
    private static final Set<Integer> ALLOWED_EDGE_DELETE_CODES = Set.of(
            GraphError.ELEMENT_NOT_FOUND.code,
            ResultCode.GENERATION_ERROR
    );
    private static final Set<Integer> ALLOWED_EDGE_WRITE_CODES = Set.of(
            GraphError.RECORD_TX_BLOCKED.code,
            GraphError.RECORD_SIZE_EXCEEDED.code
    );

    // next fields `protected` for tests
    protected final AerospikeConnection db;
    protected final FireflyGraph graph;

    public AerospikeOperations(final FireflyGraph graph) {
        this.db = graph.getBaseGraph();
        this.graph = graph;
    }

    protected Txn getOrCreateTxn() {
        // If MRTs are enabled, create a Txn.
        // However, if the current Traversal thread is within a Tinkerpop transaction, that takes priority and is
        // injected at a higher level.
        if (!db.getConfig().mrtEnabled || this.graph.tx().getCurrentTxn() != null) {
            return null;
        }

        final Txn txn = new Txn();
        txn.setTimeout(db.getConfig().mrtTimeout);
        return txn;
    }

    private FireflyTxn getOrCreateFireflyTxn() {
        final Txn txn = getOrCreateTxn();
        if (txn == null) {
            return null;
        } else {
            return new FireflyTxn(graph, txn);
        }
    }

    //////////////// VERTEX OPERATIONS ///////////////
    public FireflyVertex writeVertex(final FireflyId vertexId,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties,
                                     final boolean createOnly,
                                     final boolean isEdgeCacheOverflowed) {
        return writeVertex(vertexId, label, properties, createOnly, isEdgeCacheOverflowed, null, Optional.empty(), Optional.empty());
    }

    public List<Boolean> verticesExist(final Object[] ids) {
        final Key[] keys = new Key[ids.length];
        for (int i = 0; i < ids.length; i++) {
            final FireflyId fireflyId = graph.getIdFactory().createVertexId(ids[i]);
            keys[i] = getKey(db, db.getConfig().vertexAeroSet, fireflyId);
        }
        final boolean[] exists = db.exists(keys);
        final List<Boolean> existsList = new ArrayList<>();
        for (final boolean exist : exists) {
            existsList.add(exist);
        }
        return existsList;
    }

    /**
     * Write and construct a FireflyVertex using the provided parameters.
     *
     * @param vertexId              Id of vertex.
     * @param label                 String label of vertex.
     * @param properties            Map of properties to add to vertex.
     * @param createOnly            Flag that allows only new IDs to be written. Disable only for retry purposes.
     * @param isEdgeCacheOverflowed Initial state of edge cache to set.
     * @param partitionId           Partition ID to stage metadata update to. Only use if bulk loading, else null
     *                              (pref use other method without this param).
     * @return FireflyVertex.
     */
    public FireflyVertex writeVertex(final FireflyId vertexId,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties,
                                     final boolean createOnly,
                                     final boolean isEdgeCacheOverflowed,
                                     final Integer partitionId,
                                     final Optional<Map<String, List<FireflyId>>> toEdgeCache,
                                     final Optional<Map<String, List<FireflyId>>> fromEdgeCache) {
        Integer partition = partitionId;
        LOG.debug("Writing Vertex {} {}.", vertexId, properties);

        boolean isSuperNodeViaIncremental = false;
        final Map<Long, HashMap<Object, List<Long>>> vertexProperties = new TreeMap<>();
        final Map<Long, Map<Long, Object>> vpTypeHints = new HashMap<>();
        final List<Operation> operations = new ArrayList<>();
        Long ttlValueLong = null;

        for (final Map.Entry<String, Object> property : properties) {
            // If the property is null, skip it.
            if (property.getValue() == null) {
                continue;
            }
            // Special bulk loader property for summary updater in the case of an incremental MergeV load.
            if (property.getKey().equals(FireflyGraph.BULK_LOAD_VERTEX_ADD_KEY)) {
                partition = (Integer) property.getValue();
                continue;
            }
            // Special bulk loader property for summary updater in the case of an incremental MergeV load (is super node)
            if (property.getKey().equals(FireflyGraph.BULK_LOAD_VERTEX_ADD_KEY_IS_SUPERNODE)) {
                isSuperNodeViaIncremental = (boolean) property.getValue();
                continue;
            }
            // Handle special TTL property if flag is enabled.
            if (property.getKey().equals(TTL_PROPERTY_KEY)) {
                if (!db.getConfig().ttlEnabledFlag) {
                    throw new AerospikeGraphException(GraphError.TTL_NOT_ENABLED);
                }
                final Object ttlValue = property.getValue();
                if (Number.class.isAssignableFrom(ttlValue.getClass())) {
                    ttlValueLong = ((Number) ttlValue).longValue();
                } else {
                    throw new TtlArgumentException(ttlValue);
                }
                continue;
            }
            // Generate disk version of property.
            final Object validatedValue = validateAndConvertVertexPropertyValue(property.getValue());
            final Long schemaPropertyKey = db.schemaManager.getVertexPropertyWrite(property.getKey());
            if (!vertexProperties.containsKey(schemaPropertyKey)) {
                vertexProperties.put(schemaPropertyKey, new HashMap<>());
            }
            final Map<Object, List<Long>> propertyValueToIdList = vertexProperties.get(schemaPropertyKey);
            if (!propertyValueToIdList.containsKey(validatedValue)) {
                propertyValueToIdList.put(validatedValue, new ArrayList<>());
            }
            final FireflyId vertexPropertyId = graph.getIdFactory().generateId(graph, FireflyVertexProperty.class);
            final List<Long> idListForValue = propertyValueToIdList.get(validatedValue);
            idListForValue.add((Long) vertexPropertyId.getStorageId());
            // Generate disk version of property type hint.
            if (!vpTypeHints.containsKey(schemaPropertyKey)) {
                vpTypeHints.put(schemaPropertyKey, new HashMap<>());
            }
            final Map<Long, Object> propertyIdToTypeHint = vpTypeHints.get(schemaPropertyKey);
            propertyIdToTypeHint.put((Long) vertexPropertyId.getStorageId(), getTypeHintOf(property.getValue(), true));
        }

        if (ttlValueLong != null) {
            final long expirationTime = System.currentTimeMillis() + (ttlValueLong * 1000);
            final Bin ttlBin = new Bin(db.getConfig().ttlBin, expirationTime);
            final Operation writeTtlBin = Operation.put(ttlBin);
            operations.add(writeTtlBin);
        }

        // Create vertex bins for cache state, vertex label, and property ids.
        final Bin cacheDisabledBin = new Bin(db.getConfig().edgeCacheDisabledBin, Value.get(isEdgeCacheOverflowed));
        final Operation writeCacheDisabled = Operation.put(cacheDisabledBin);
        final Bin labelBin = new Bin(db.getConfig().labelBin, Value.get(db.schemaManager.getVertexLabelWrite(label)));
        final Operation writeLabel = Operation.put(labelBin);

        final Operation writeEdgeCacheOut = getEdgeCache(fromEdgeCache, db.getConfig().outEdgesBin);
        final Operation writeEdgeCacheIn = getEdgeCache(toEdgeCache, db.getConfig().inEdgesBin);

        final Bin vertexPropertiesBin = new Bin(db.getConfig().vertexPropertyDataBin, Value.get(vertexProperties));
        final Operation writeVertexProperties = Operation.put(vertexPropertiesBin);
        final Bin vpTypeHintsBin = new Bin(db.getConfig().vertexPropertyTHBin, Value.get(vpTypeHints));
        final Operation writeVpTypeHints = Operation.put(vpTypeHintsBin);

        // Create Vertex Property Properties maps.
        // The existence of the Vertex Property ID as a key in this map is what is used to determine whether the
        // Vertex Property currently exists, and thus instantiating it here is necessary.
        final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties = new HashMap<>();
        for (final Map.Entry<Long, HashMap<Object, List<Long>>> vertexProperty : vertexProperties.entrySet()) {
            final Map<Object, List<Long>> vpValueToIdList = vertexProperty.getValue();
            final Map<Long, Map<Long, List<Object>>> vpPropertiesForKey = new HashMap<>();
            for (final List<Long> vpIds : vpValueToIdList.values()) {
                for (final Long vpId : vpIds) {
                    vpPropertiesForKey.put(vpId, new HashMap<>());
                }
            }
            vpProperties.put(vertexProperty.getKey(), vpPropertiesForKey);
        }

        final Bin vpPropertiesBin = new Bin(db.getConfig().vpPropertyBin, Value.get(vpProperties));
        final Operation writeVpProperties = Operation.put(vpPropertiesBin);
        final Bin idTypeBin = new Bin(db.getConfig().idTypeBin, Value.get(vertexId.getStorageTypeHint()));
        final Operation writeIdTypeHint = Operation.put(idTypeBin);

        final Key key = getKey(db, db.getConfig().vertexAeroSet, vertexId);
        final WritePolicy policy = new WritePolicy();
        if (createOnly) {
            policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        }

        final Bin userKeyBin = new Bin(db.getConfig().userKeyBin, Value.get(vertexId.getStorageId()));
        final Operation writeUserKey = Operation.put(userKeyBin);
        operations.add(writeUserKey);

        operations.add(writeCacheDisabled);
        operations.add(writeLabel);
        operations.add(writeEdgeCacheIn);
        operations.add(writeEdgeCacheOut);
        operations.add(writeVertexProperties);
        operations.add(writeVpTypeHints);
        operations.add(writeVpProperties);
        operations.add(writeIdTypeHint);

        db.writeOperate(policy, key, operations.toArray(new Operation[0]));
        if (partition == null) {
            graph.fireflySummaryUpdater.addVertexWriteToQueue(label, vertexProperties.keySet().stream().
                    map(db.schemaManager::getVertexPropertyString).collect(Collectors.toSet()), graph.tx().getCurrentTxn());
        } else {
            graph.fireflySummaryUpdater.stageVertexWriteToQueue(label, vertexProperties.keySet().stream().
                    map(db.schemaManager::getVertexPropertyString).collect(Collectors.toSet()), partition);
            // Increase the supernode counter for fresh/incremental bulk loader (newly added vertices).
            if (isEdgeCacheOverflowed || isSuperNodeViaIncremental) {
                graph.fireflySummaryUpdater.stageSupernodeWriteToQueue(label, partition);
            }
        }
        final FireflyVertex vertex = FireflyVertexFactory.create(vertexId, label, graph, new TreeMap<>(),
                new TreeMap<>(), vertexProperties, vpTypeHints, vpProperties, isEdgeCacheOverflowed);
        return vertex;
    }

    private Operation getEdgeCache(final Optional<Map<String, List<FireflyId>>> optionalEdgeCache, final String bin) {
        if (optionalEdgeCache.isEmpty()) {
            final Map<String, List<Long>> emptyEdgeCache = new TreeMap<>();
            final Bin edgeCacheBin = new Bin(bin, Value.get(emptyEdgeCache));
            return Operation.put(edgeCacheBin);
        }
        final Map<String, List<FireflyId>> edgeCacheMap = optionalEdgeCache.get();
        final Map<String, List<Value>> vertexEdgeMap = new TreeMap<>();
        for (final Map.Entry<String, List<FireflyId>> entry : edgeCacheMap.entrySet()) {
            vertexEdgeMap.put(entry.getKey(), entry.getValue().stream().map(e -> Value.get(e.getCachedId())).collect(Collectors.toList()));
        }
        final Bin edgeCacheBin = new Bin(bin, Value.get(vertexEdgeMap));
        return Operation.put(edgeCacheBin);
    }

    /**
     * Get a Vertex directly via ID, bypassing the TTL pushdown filter.
     *
     * @param vertexId ID of Vertex to return
     * @return Vertex
     */
    public FireflyVertex getSingleVertex(final FireflyId vertexId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.getConfig().vertexAeroSet, vertexId);
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
        final Key key = getKey(this.db, this.db.getConfig().vertexAeroSet, vertex.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.db.writeOperate(writePolicy, key,
                Operation.put(new Bin(this.db.getConfig().edgeCacheDisabledBin, Value.get(true))));
        vertex.setIsEdgeCacheOverflowed(true);
    }

    public void setTtl(final FireflyVertex vertex, final long durationSeconds) {
        final long expirationTime = System.currentTimeMillis() + (durationSeconds * 1000);
        final Key key = getKey(this.db, this.db.getConfig().vertexAeroSet, vertex.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        final Bin ttlBin = new Bin(this.db.getConfig().ttlBin, expirationTime);
        final Operation writeTtl = Operation.put(ttlBin);
        this.db.writeOperate(writePolicy, key, writeTtl);
    }

    public Iterator<KeyRecord> getEdgeKeyRecordsByIndex(final FireflyId vertexId,
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
        queryPolicy.includeBinData = true;
        queryPolicy.filterExp = EdgeQueryHelper.phatEdgeHasContainerListToExpression(db, hasContainers, labels,
                vertexId, adjacentVertexId, direction);
        if (direction == Direction.OUT) {
            return new CachedIterator(graph, graph.graphQuery.querySIndex(db.getConfig().edgeAeroSet, db.getConfig().eOutIndexName,
                    Filter.contains(db.getConfig().supernodesOutBin, IndexCollectionType.MAPKEYS, vertexId.getKeyHashString()),
                    queryPolicy));
        } else if (direction == Direction.IN) {
            return new CachedIterator(graph, graph.graphQuery.querySIndex(db.getConfig().edgeAeroSet, db.getConfig().eInIndexName,
                    Filter.contains(db.getConfig().supernodesInBin, IndexCollectionType.MAPKEYS, vertexId.getKeyHashString()),
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
            this.cache = graph.getBaseGraph().cacheManager.getTransactionCache();
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
        final FireflyTxn fireflyTxn = getOrCreateFireflyTxn();
        try {
            if (fireflyTxn != null && vertex.isEdgeCacheOverflowed()) {
                // If the vertex is a supernode we can't remove its edges within the limit of a single MRT.
                LOG.warn("Dropping supernode Vertex {}. All attached Edges will be dropped but some not as part of the MRT.", vertex.id());

                // Remove supernode edges themselves and from the adjacent vertices' edge caches outside the MRT.
                final Iterator<FireflyEdge> sindexedInEdges = new FireflyBatchEdgeIterator<>(graph, vertex.getSupernodeEdgeIds(Direction.IN, Collections.emptySet(), Collections.emptyList()));
                final Iterator<FireflyEdge> sindexedOutEdges = new FireflyBatchEdgeIterator<>(graph, vertex.getSupernodeEdgeIds(Direction.OUT, Collections.emptySet(), Collections.emptyList()));
                sindexedInEdges.forEachRemaining(e -> removeEdge(e, true, false, null));
                sindexedOutEdges.forEachRemaining(e -> removeEdge(e, false, true, null));

                // Remove cached edges themselves and from the adjacent vertices' edge caches within the MRT.
                final Iterator<FireflyEdge> cachedInEdges = new FireflyBatchEdgeIterator<>(graph, vertex.getCachedEdgeIds(Direction.IN, Collections.emptySet()).iterator());
                final Iterator<FireflyEdge> cachedOutEdges = new FireflyBatchEdgeIterator<>(graph, vertex.getCachedEdgeIds(Direction.OUT, Collections.emptySet()).iterator());
                cachedInEdges.forEachRemaining(e -> removeEdge(e, true, false, fireflyTxn));
                cachedOutEdges.forEachRemaining(e -> removeEdge(e, false, true, fireflyTxn));
            } else {
                // Collect edges in both directions and remove them all.
                final Iterator<FireflyEdge> inEdges = new FireflyBatchEdgeIterator<>(graph, vertex.getEdgeIdsFromVertex(Direction.IN, Collections.emptySet(), Collections.emptyList()));
                final Iterator<FireflyEdge> outEdges = new FireflyBatchEdgeIterator<>(graph, vertex.getEdgeIdsFromVertex(Direction.OUT, Collections.emptySet(), Collections.emptyList()));
                // Remove the edges themselves and from the adjacent vertices' edge caches.
                inEdges.forEachRemaining(e -> removeEdge(e, true, false, fireflyTxn));
                outEdges.forEachRemaining(e -> removeEdge(e, false, true, fireflyTxn));
            }

            // Remove vertex.
            LOG.debug("Removing vertex {}.", vertex.id);
            final Txn txn = fireflyTxn != null ? fireflyTxn.aerospikeTxn : graph.tx().getCurrentTxn();
            if (db.delete(FireflyRecord.getKey(db, db.getConfig().vertexAeroSet, vertex.id), txn)) {
                graph.fireflySummaryUpdater.addVertexRemoveToQueue(vertex.label(), txn);

                // If removed vertex was a supernode, decrease counter
                if (vertex.isEdgeCacheOverflowed()) {
                    graph.fireflySummaryUpdater.addSupernodeRemoveToQueue(vertex.label(), txn);
                }
            }

            commit(fireflyTxn);

            if (graph.getBaseGraph().getConfig().isAuditLogEnabled) {
                LOG.info("[{}] Dropped vertex with id: {}", graph.getUser(), vertex.id());
            }
        } catch (final RuntimeException e) {
            rollback(fireflyTxn);
            throw e;
        }
    }

    //////////////// VERTEX PROPERTIES ///////////////

    /**
     * Write property to Vertex Property.
     *
     * @param propertyKey   Key of property to write.
     * @param propertyValue Value of property to write.
     * @param <F>           Type of property value.
     * @return Property written to Vertex Property.
     */
    public <F> Property<F> writeVpProperty(final FireflyVertexProperty vertexProperty, final String propertyKey,
                                           final F propertyValue) {
        final Key opKey = getKey(db, db.getConfig().vertexAeroSet, vertexProperty.vertexId);
        final Long schemaVpKey = db.schemaManager.getVertexPropertyWrite(vertexProperty.key());
        final Long vpId = (Long) vertexProperty.id.getStorageId();
        final Long schemaPropertyKey = db.schemaManager.getVpPropertyWrite(propertyKey);
        final List<Operation> operations = new ArrayList<>();

        final List<Object> valueAndTypeHint = new ArrayList<>(2);
        final Object propertyValueToWrite = FireflyHelper.convertValueToAerospikeWriteable(propertyValue);
        valueAndTypeHint.add(propertyValueToWrite);
        valueAndTypeHint.add(getTypeHintOf(propertyValue));
        final MapPolicy policy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
        final Operation writeValue = MapOperation.put(policy, db.getConfig().vpPropertyBin, Value.get(schemaPropertyKey),
                Value.get(valueAndTypeHint), CTX.mapKey(Value.get(schemaVpKey)), CTX.mapKey(Value.get(vpId)));
        operations.add(writeValue);

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
        vertexProperty.properties.put(schemaPropertyKey, valueAndTypeHint);
        return new FireflyVertexPropertyProperty<>(graph, vertexProperty, propertyKey, propertyValue);
    }

    public void appendVpProperties(final FireflyVertexProperty vertexProperty,
                                   final Map<Long, List<Object>> properties) {
        final Key opKey = getKey(db, db.getConfig().vertexAeroSet, vertexProperty.vertexId);
        final Long schemaVpKey = db.schemaManager.getVertexPropertyWrite(vertexProperty.key());
        final Long vpId = (Long) vertexProperty.id.getStorageId();

        final Map<Value, Value> propertiesValueMap = new HashMap<>();
        properties.forEach((propertyKey, propertyValue) -> {
            propertiesValueMap.put(Value.get(propertyKey), Value.get(propertyValue));
        });

        final MapPolicy policy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
        final Operation writeValuesOp = MapOperation.putItems(policy, db.getConfig().vpPropertyBin, propertiesValueMap,
                CTX.mapKey(Value.get(schemaVpKey)), CTX.mapKey(Value.get(vpId)));

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, opKey, Set.of(ResultCode.OP_NOT_APPLICABLE, ResultCode.KEY_NOT_FOUND_ERROR),
                    false, writeValuesOp);
        } catch (final AerospikeGraphRecordSizeExceededException rtbe) {
            if (properties.isEmpty()) {
                // This should never happen
                throw new IllegalStateException("Attempted to write empty VP Property and exceeded record size. Please contact support.");
            }
            // This will basically almost never happen in practice, so just return the first key as the source of error.
            final String propertyKeyString = this.db.schemaManager.getVpPropertyString(properties.keySet().iterator().next());
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVpProperty((AerospikeException) rtbe.getCause(), db, getRelevantVertexBins(db, opKey),
                            vertexProperty.vertexId, vertexProperty.key(), propertyKeyString);
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
    }

    /**
     * Write Vertex Property to Vertex.
     *
     * @param cardinality Cardinality of Vertex Property.
     * @param vertex      Parent Vertex of the Vertex Property.
     * @param key         Vertex Property key.
     * @param value       Vertex Property value.
     * @param properties  Vertex Property Properties.
     * @param <V>         Vertex Property value type.
     * @return Newly written Vertex Property.
     */
    public <V> VertexProperty<V> writeVertexProperty(final VertexProperty.Cardinality cardinality,
                                                     final FireflyVertex vertex,
                                                     final String key,
                                                     final V value,
                                                     final Map<Long, List<Object>> properties) {
        final Key recordKey = getKey(this.db, this.db.getConfig().vertexAeroSet, vertex.id);
        final List<Operation> operations = new ArrayList<>();

        final FireflyId vertexPropertyId =
                appendWriteVertexPropertyOps(operations, cardinality, key, value, properties);
        appendGetVertexPropertyBinOps(operations);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record result = this.db.writeOperate(writePolicy, recordKey, operations.toArray(new Operation[0]));
            final Map<Long, HashMap<Object, List<Long>>> vertexProperties =
                    (Map<Long, HashMap<Object, List<Long>>>) getValueAtIndex(result, this.db.getConfig().vertexPropertyDataBin, 1);
            final Map<Long, Map<Long, Object>> vpTypeHints =
                    (Map<Long, Map<Long, Object>>) getValueAtIndex(result, this.db.getConfig().vertexPropertyTHBin, 1);
            final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties =
                    (Map<Long, Map<Long, Map<Long, List<Object>>>>) getValueAtIndex(result, this.db.getConfig().vpPropertyBin, 1);

            // Update this FireflyVertex in JVM cache
            vertex.updateVertexPropertyJVMCache(vertexProperties, vpTypeHints, vpProperties);
            if (vertexPropertyId == null) {
                // If vertexPropertyId is null it means it was a Cardinality single value null property (removal)
                return VertexProperty.empty();
            } else if (cardinality.equals(VertexProperty.Cardinality.set)) {
                // If Cardinality.set we need to check if the new VP was added to the set (value didn't exist already)
                final VertexProperty<V> matchedProperty = vertex.postCardinalitySetPropertyWrite(key, value, properties,
                        vertexPropertyId);
                // If the matched property doesn't have the same ID it means it previously existed in the set so skip
                // updating the summary and return the existing property
                if (!matchedProperty.id().equals(vertexPropertyId.getUserId())) {
                    return matchedProperty;
                }
            }
            graph.fireflySummaryUpdater.addVertexPropertiesWriteToQueue(vertex.label(), Set.of(key),
                    graph.tx().getCurrentTxn());
            return new FireflyVertexProperty<>(graph, vertexPropertyId, vertex, key, value, properties);
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVertexProperty((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, recordKey), vertex.id, key);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    public void batchWriteVertexProperties(final FireflyVertex vertex,
                                           final List<VertexProperty.Cardinality> cardinalities,
                                           final List<String> keys,
                                           final List<Object> values) {
        final List<Operation> operations = new ArrayList<>();
        final Set<String> writtenKeys = new HashSet<>();
        final int propertyCount = cardinalities.size();
        for (int i = 0; i < propertyCount; i++) {
            final FireflyId vertexPropertyId = appendWriteVertexPropertyOps(operations, cardinalities.get(i),
                    keys.get(i), values.get(i), Collections.emptyMap());
            if (vertexPropertyId != null) {
                writtenKeys.add(keys.get(i));
            }
        }
        appendGetVertexPropertyBinOps(operations);

        final Key recordKey = getKey(this.db, this.db.getConfig().vertexAeroSet, vertex.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record result = this.db.writeOperate(writePolicy, recordKey, operations.toArray(new Operation[0]));
            final Map<Long, HashMap<Object, List<Long>>> vertexProperties =
                    (Map<Long, HashMap<Object, List<Long>>>) getValueAtIndex(result, this.db.getConfig().vertexPropertyDataBin, propertyCount);
            final Map<Long, Map<Long, Object>> vpTypeHints =
                    (Map<Long, Map<Long, Object>>) getValueAtIndex(result, this.db.getConfig().vertexPropertyTHBin, propertyCount);
            final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties =
                    (Map<Long, Map<Long, Map<Long, List<Object>>>>) getValueAtIndex(result, this.db.getConfig().vpPropertyBin, propertyCount);

            // Update this FireflyVertex in JVM cache
            vertex.updateVertexPropertyJVMCache(vertexProperties, vpTypeHints, vpProperties);
            graph.fireflySummaryUpdater.addVertexPropertiesWriteToQueue(vertex.label(), writtenKeys,
                    graph.tx().getCurrentTxn());
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVertexProperty((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, recordKey), vertex.id, String.join(", ", writtenKeys));
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    private FireflyId appendWriteVertexPropertyOps(final List<Operation> operations,
                                                   final VertexProperty.Cardinality cardinality,
                                                   final String key,
                                                   final Object value,
                                                   final Map<Long, List<Object>> properties) {
        if (value == null) {
            if (cardinality != VertexProperty.Cardinality.single) {
                // This should never happen.
                throw new IllegalArgumentException("Null Vertex Property values are invalid when Cardinality is single. Please contact support.");
            }
            final Long schemaKey = db.schemaManager.getVertexPropertyRead(key);
            final Operation removeVpKey = MapOperation.removeByKey(this.db.getConfig().vertexPropertyDataBin, Value.get(schemaKey), MapReturnType.NONE);
            operations.add(removeVpKey);
            final Operation removeTypeHintKey = MapOperation.removeByKey(this.db.getConfig().vertexPropertyTHBin, Value.get(schemaKey), MapReturnType.NONE);
            operations.add(removeTypeHintKey);
            final Operation removeVpPropertiesKey = MapOperation.removeByKey(this.db.getConfig().vpPropertyBin, Value.get(schemaKey), MapReturnType.NONE);
            operations.add(removeVpPropertiesKey);
            return null;
        } else {
            final FireflyId vertexPropertyId = this.db.getIdFactory().generateId(this.graph, FireflyVertexProperty.class);
            final Long vpIdKey = (Long) vertexPropertyId.getStorageId();
            final Long schemaVpKey = this.db.schemaManager.getVertexPropertyWrite(key);
            final Object typeHint = getTypeHintOf(value, true);
            final Object verifiedValue = validateAndConvertVertexPropertyValue(value);

            final Operation writeVpData;
            final Operation writeVpTypeHint;
            final Operation writeVpProperties;

            final MapPolicy treeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            final MapPolicy hashMapPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
            final ListPolicy preventDuplicates = new ListPolicy(ListOrder.UNORDERED,
                    ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);
            if (cardinality.equals(VertexProperty.Cardinality.single)) {
                final List<Long> idInList = new ArrayList<>(1);
                idInList.add(vpIdKey);
                final Map<Object, List<Long>> valueToIdList = new HashMap<>();
                valueToIdList.put(verifiedValue, idInList);
                writeVpData = MapOperation.put(treeMapPolicy, this.db.getConfig().vertexPropertyDataBin, Value.get(schemaVpKey), Value.get(valueToIdList));
                final Map<Long, Object> idToTypeHint = new HashMap<>();
                idToTypeHint.put(vpIdKey, typeHint);
                writeVpTypeHint = MapOperation.put(hashMapPolicy, this.db.getConfig().vertexPropertyTHBin, Value.get(schemaVpKey), Value.get(idToTypeHint));
                final Map<Long, Map<Long, List<Object>>> idToProperties = new HashMap<>();
                idToProperties.put(vpIdKey, properties);
                writeVpProperties = MapOperation.put(hashMapPolicy, this.db.getConfig().vpPropertyBin, Value.get(schemaVpKey), Value.get(idToProperties));
            } else if (cardinality.equals(VertexProperty.Cardinality.list)) {
                writeVpData = ListOperation.append(preventDuplicates, this.db.getConfig().vertexPropertyDataBin, Value.get(vpIdKey),
                        CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.KEY_ORDERED), CTX.mapKeyCreate(Value.get(verifiedValue), MapOrder.UNORDERED));
                writeVpTypeHint = MapOperation.put(hashMapPolicy, this.db.getConfig().vertexPropertyTHBin, Value.get(vpIdKey),
                        Value.get(typeHint), CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.UNORDERED));
                writeVpProperties = MapOperation.put(hashMapPolicy, this.db.getConfig().vpPropertyBin, Value.get(vpIdKey),
                        Value.get(properties), CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.UNORDERED));
            } else if (cardinality.equals(VertexProperty.Cardinality.set)) {
                final Expression writeVpPropertiesExp = Exp.build(Exp.cond(
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(schemaVpKey),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin))),
                        MapExp.put(hashMapPolicy, getExpVal(vpIdKey), Exp.val(properties),
                                Exp.mapBin(this.db.getConfig().vpPropertyBin),
                                CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.UNORDERED)),
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, getExpVal(verifiedValue),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin),
                                CTX.mapKey(Value.get(schemaVpKey)))),
                        MapExp.put(hashMapPolicy, getExpVal(vpIdKey), Exp.val(properties),
                                Exp.mapBin(this.db.getConfig().vpPropertyBin),
                                CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.UNORDERED)),
                        Exp.unknown()
                ));
                writeVpProperties = ExpOperation.write(this.db.getConfig().vpPropertyBin, writeVpPropertiesExp, ExpWriteFlags.EVAL_NO_FAIL);
                final Expression writeTypeHintExp = Exp.build(Exp.cond(
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(schemaVpKey),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin))),
                        MapExp.put(hashMapPolicy, getExpVal(vpIdKey), getExpVal(typeHint),
                                Exp.mapBin(this.db.getConfig().vertexPropertyTHBin),
                                CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.UNORDERED)),
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, getExpVal(verifiedValue),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin),
                                CTX.mapKey(Value.get(schemaVpKey)))),
                        MapExp.put(hashMapPolicy, getExpVal(vpIdKey), getExpVal(typeHint),
                                Exp.mapBin(this.db.getConfig().vertexPropertyTHBin),
                                CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.UNORDERED)),
                        Exp.unknown()
                ));
                writeVpTypeHint = ExpOperation.write(this.db.getConfig().vertexPropertyTHBin, writeTypeHintExp, ExpWriteFlags.EVAL_NO_FAIL);
                final List<Long> idInList = new ArrayList<>(1);
                idInList.add(vpIdKey);
                final Expression writeVpDataExp = Exp.build(Exp.cond(
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(schemaVpKey),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin))),
                        MapExp.put(hashMapPolicy, getExpVal(verifiedValue), Exp.val(idInList),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin),
                                CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.KEY_ORDERED)),
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, getExpVal(verifiedValue),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin),
                                CTX.mapKey(Value.get(schemaVpKey)))),
                        MapExp.put(hashMapPolicy, getExpVal(verifiedValue), Exp.val(idInList),
                                Exp.mapBin(this.db.getConfig().vertexPropertyDataBin),
                                CTX.mapKeyCreate(Value.get(schemaVpKey), MapOrder.KEY_ORDERED)),
                        Exp.unknown()
                ));
                writeVpData = ExpOperation.write(this.db.getConfig().vertexPropertyDataBin, writeVpDataExp, ExpWriteFlags.EVAL_NO_FAIL);
            } else {
                // This should never happen.
                throw new IllegalArgumentException("Invalid Cardinality '" + cardinality + "' was provided. Please contact support.");
            }

            if (cardinality.equals(VertexProperty.Cardinality.set)) {
                // Write the VP data itself last when using Cardinality.set since we're using it to check uniqueness.
                operations.add(writeVpProperties);
                operations.add(writeVpTypeHint);
                operations.add(writeVpData);
            } else {
                operations.add(writeVpData);
                operations.add(writeVpTypeHint);
                operations.add(writeVpProperties);
            }
            return vertexPropertyId;
        }
    }

    private void appendGetVertexPropertyBinOps(final List<Operation> operations) {
        final Operation getVertexPropertyValues = Operation.get(this.db.getConfig().vertexPropertyDataBin);
        operations.add(getVertexPropertyValues);
        final Operation getVertexPropertyTypeHints = Operation.get(this.db.getConfig().vertexPropertyTHBin);
        operations.add(getVertexPropertyTypeHints);
        final Operation getVertexPropertyProperties = Operation.get(this.db.getConfig().vpPropertyBin);
        operations.add(getVertexPropertyProperties);
    }

    /**
     * Remove the Vertex Property from the Vertex.
     *
     * @param vertex           Parent Vertex of the Vertex Property.
     * @param key              Vertex Property key.
     * @param value            Vertex Property value.
     * @param vertexPropertyId FireflyId of the Vertex Property to remove.
     */
    public void removeVertexProperty(final FireflyVertex vertex, final String key, final Object value,
                                     final FireflyId vertexPropertyId) {
        final Key opKey = getKey(this.db, this.db.getConfig().vertexAeroSet, vertex.id);
        final Long schemaKey = db.schemaManager.getVertexPropertyRead(key);
        final Long vpIdKey = (Long) vertexPropertyId.getStorageId();
        final Object validatedValue = validateAndConvertVertexPropertyValue(value);
        final Exp vpValueExp = getExpVal(validatedValue);
        final List<Operation> operations = new ArrayList<>();

        // Remove Vertex Property.
        final Operation removeVertexProperty = ListOperation.removeByValue(this.db.getConfig().vertexPropertyDataBin,
                Value.get(vpIdKey), ListReturnType.NONE, CTX.mapKey(Value.get(schemaKey)), CTX.mapKey(Value.get(validatedValue)));
        operations.add(removeVertexProperty);
        // Logic for deleting Vertex Property value key if there are no VPs left with the value.
        final Expression removeVpValueKeyExp = Exp.build(
                Exp.cond(
                        Exp.eq(ListExp.size(Exp.mapBin(this.db.getConfig().vertexPropertyDataBin), CTX.mapKey(Value.get(schemaKey)), CTX.mapKey(Value.get(validatedValue))), Exp.val(0)),
                        MapExp.removeByKey(vpValueExp, Exp.mapBin(this.db.getConfig().vertexPropertyDataBin), CTX.mapKey(Value.get(schemaKey))),
                        Exp.unknown()
                )
        );
        final Operation removeVpValueKey = ExpOperation.write(db.getConfig().vertexPropertyDataBin, removeVpValueKeyExp, ExpWriteFlags.EVAL_NO_FAIL);
        operations.add(removeVpValueKey);
        // Logic for deleting Vertex Property key if there are no VPs left for the key.
        final Expression removeVpKeyExp = Exp.build(
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(this.db.getConfig().vertexPropertyDataBin), CTX.mapKey(Value.get(schemaKey))), Exp.val(0)),
                        MapExp.removeByKey(Exp.val(schemaKey), Exp.mapBin(this.db.getConfig().vertexPropertyDataBin)),
                        Exp.unknown()
                )
        );
        final Operation removeVpKey = ExpOperation.write(db.getConfig().vertexPropertyDataBin, removeVpKeyExp, ExpWriteFlags.EVAL_NO_FAIL);
        operations.add(removeVpKey);
        // Remove Type Hint.
        final Operation removeVertexPropertyTypeHint = MapOperation.removeByKey(this.db.getConfig().vertexPropertyTHBin,
                Value.get(vpIdKey), MapReturnType.NONE, CTX.mapKey(Value.get(schemaKey)));
        operations.add(removeVertexPropertyTypeHint);
        // Logic for deleting Vertex Property key if there are no type hints left for the key.
        final Expression removeVpKeyTypeHintExp = Exp.build(
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(this.db.getConfig().vertexPropertyTHBin), CTX.mapKey(Value.get(schemaKey))), Exp.val(0)),
                        MapExp.removeByKey(Exp.val(schemaKey), Exp.mapBin(this.db.getConfig().vertexPropertyTHBin)),
                        Exp.unknown()
                )
        );
        final Operation removeVpKeyTypeHint = ExpOperation.write(db.getConfig().vertexPropertyTHBin, removeVpKeyTypeHintExp, ExpWriteFlags.EVAL_NO_FAIL);
        operations.add(removeVpKeyTypeHint);
        // Remove Vertex Property's Properties.
        final Operation removeProperty = MapOperation.removeByKey(this.db.getConfig().vpPropertyBin, Value.get(vpIdKey),
                MapReturnType.NONE, CTX.mapKey(Value.get(schemaKey)));
        operations.add(removeProperty);
        // Logic for deleting Vertex Property key if there are no VP Properties left for the key.
        final Expression removeVpKeyPropertyExp = Exp.build(
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(this.db.getConfig().vpPropertyBin), CTX.mapKey(Value.get(schemaKey))), Exp.val(0)),
                        MapExp.removeByKey(Exp.val(schemaKey), Exp.mapBin(this.db.getConfig().vpPropertyBin)),
                        Exp.unknown()
                )
        );
        final Operation removeVpKeyProperty = ExpOperation.write(db.getConfig().vpPropertyBin, removeVpKeyPropertyExp, ExpWriteFlags.EVAL_NO_FAIL);
        operations.add(removeVpKeyProperty);

        appendGetVertexPropertyBinOps(operations);

        Map<Long, HashMap<Object, List<Long>>> vertexProperties = Collections.emptyMap();
        Map<Long, Map<Long, Object>> vpTypeHints = Collections.emptyMap();
        Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties = Collections.emptyMap();
        try {
            final Record result = this.db.writeOperate(null, opKey, operations.toArray(new Operation[]{}));

            vertexProperties =
                    (Map<Long, HashMap<Object, List<Long>>>) getValueAtIndex(result, this.db.getConfig().vertexPropertyDataBin, 3);
            vpTypeHints =
                    (Map<Long, Map<Long, Object>>) getValueAtIndex(result, this.db.getConfig().vertexPropertyTHBin, 2);
            vpProperties =
                    (Map<Long, Map<Long, Map<Long, List<Object>>>>) getValueAtIndex(result, this.db.getConfig().vpPropertyBin, 2);
            // Update this FireflyVertex in JVM cache
            vertex.updateVertexPropertyJVMCache(vertexProperties, vpTypeHints, vpProperties);
        } catch (final AerospikeGraphException ae) {
            // Removing a property that is already removed SHOULD NOT yield an error.
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE || ae.errorCode == ResultCode.KEY_NOT_FOUND_ERROR) {
                LOG.debug("Ignored exception removing an already-removed vertex property {}.",
                        new FireflyVertexProperty<>(graph, vertexPropertyId, vertex, key, value, Collections.emptyMap()), ae);
                // Update this FireflyVertex in JVM cache
                vertex.updateVertexPropertyJVMCache(vertexProperties, vpTypeHints, vpProperties);
            } else {
                throw ae;
            }
        }
    }

    private Exp getExpVal(final Object value) {
        if (value == null) {
            return Exp.nil();
        }
        final Class<?> valueClass = value.getClass();
        final Exp valueExp;
        if (Long.class.isAssignableFrom(valueClass)) {
            valueExp = Exp.val((Long) value);
        } else if (Integer.class.isAssignableFrom(valueClass)) {
            valueExp = Exp.val(((Integer) value).longValue());
        } else if (Double.class.isAssignableFrom(valueClass)) {
            valueExp = Exp.val((Double) value);
        } else if (String.class.isAssignableFrom(valueClass)) {
            valueExp = Exp.val((String) value);
        } else if (Boolean.class.isAssignableFrom(valueClass)) {
            valueExp = Exp.val((Boolean) value);
        } else if (byte[].class.isAssignableFrom(valueClass)) {
            valueExp = Exp.val((byte[]) value);
        } else {
            // This should never happen.
            throw new IllegalArgumentException("Attempted to get Exp value of unsupported type '" + valueClass.getSimpleName() + "'. Please contact support.");
        }
        return valueExp;
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

        final List<Operation> operations = new ArrayList<>();
        final boolean isAttachedToSupernode = !inVertexCacheWrite || !outVertexCacheWrite;
        // CREATE_ONLY as writing an edge will always have a newly-generated unique ID.
        final MapPolicy edgeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED,
                MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL | MapWriteFlags.PARTIAL);
        final Value edgeIdkey = Value.get(((FireflyEdgeId) edgeId).getEdgeIdBytes());

        final Map<String, Object> propertyMap = new TreeMap<>();
        final Map<String, Object> typeHints = new HashMap<>();
        properties.forEach(property -> {
            final String key = property.getKey();
            final Object originalValue = FireflyHelper.validatePropertyValue(property.getValue());
            final Object aerospikeWritableValue = FireflyHelper.convertValueToAerospikeWriteable(originalValue);

            if (originalValue == null) {
                propertyMap.remove(key);
                typeHints.remove(key);
            } else {
                if (key.equals(TTL_PROPERTY_KEY)) {
                    if (!db.getConfig().ttlEnabledFlag) {
                        throw new AerospikeGraphException(GraphError.TTL_NOT_ENABLED);
                    }
                    if (Number.class.isAssignableFrom(originalValue.getClass())) {
                        final long ttlValueLong = ((Number) originalValue).longValue();
                        final long expirationTime = System.currentTimeMillis() + (ttlValueLong * 1000);
                        final Operation writeTtl = MapOperation.put(edgeMapPolicy, db.getConfig().ttlBin, edgeIdkey,
                                Value.get(expirationTime));
                        operations.add(writeTtl);
                    } else {
                        throw new TtlArgumentException(originalValue);
                    }
                } else {
                    final Object typeHint = getTypeHintOf(originalValue);
                    if (typeHint != null) {
                        typeHints.put(key, typeHint);
                    }
                    propertyMap.put(key, aerospikeWritableValue);
                }
            }
        });
        final HashMap<Long, Object> typeHintsDisk = new HashMap<>();
        db.schemaManager.populateEdgePropertyStringMapToSchemaMap(typeHints, typeHintsDisk);

        final List<Map.Entry<String, Object>> validProperties = new ArrayList<>();
        propertyMap.forEach((propertyKey, propertyValue) ->
                validProperties.add(new AbstractMap.SimpleEntry<>(propertyKey, propertyValue)));

        if (!isAttachedToSupernode) {
            // If the Edge is not attached to a supernode, store its data in a List
            final List<Value> edgeData = new ArrayList<>(EDGE_DATA_SIZE);
            // Add label to Edge data.
            edgeData.add(LABEL_POSITION, Value.get(db.schemaManager.getEdgeLabelWrite(label)));
            // Add IN and OUT to Edge data.
            edgeData.add(IN_V_POSITION, Value.get(inVertex.id.getUserId()));
            edgeData.add(OUT_V_POSITION, Value.get(outVertex.id.getUserId()));

            // Add properties and type hints to Edge data.
            final TreeMap<Long, Object> propertyMapDisk = new TreeMap<>();
            db.schemaManager.populateEdgePropertyStringMapToSchemaMap(propertyMap, propertyMapDisk);
            edgeData.add(PROPERTIES_POSITION, Value.get(propertyMapDisk));
            edgeData.add(TYPE_HINTS_POSITION, Value.get(typeHintsDisk));

            // Create Operation for writing Edge data.
            final Operation createIndividualEdgeMap = MapOperation.put(edgeMapPolicy, db.getConfig().edgeDataBin,
                    edgeIdkey, Value.get(edgeData));
            operations.add(createIndividualEdgeMap);
        } else {
            // If the Edge is attached to a supernode, rest of the data has to exist elsewhere so store only type hint
            final Operation createEdgeToTypeHint = MapOperation.put(edgeMapPolicy, db.getConfig().edgeDataBin, edgeIdkey,
                    Value.get(typeHintsDisk));
            operations.add(createEdgeToTypeHint);
        }

        // Write to filterable supernode bin if necessary.
        operations.addAll(createFilterableSupernodeOperations((FireflyPhatEdgeId) edgeId, !outVertexCacheWrite,
                !inVertexCacheWrite, outVertex.id, inVertex.id, label, propertyMap));

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.txn = txn;
        final Key key = getKey(db, db.getConfig().edgeAeroSet, edgeId);
        try {
            final Record record = db.writeOperate(writePolicy, key, ALLOWED_EDGE_WRITE_CODES, false, operations.toArray(new Operation[0]));
            final Txn summaryTxn = txn != null ? txn : graph.tx().getCurrentTxn();
            graph.fireflySummaryUpdater.addEdgeWriteToQueue(label, validProperties.stream().map(Map.Entry::getKey)
                    .collect(Collectors.toSet()), summaryTxn);
            return FireflyEdgeFactory.create(edgeId, label, graph, outVertex.id, inVertex.id,
                    propertyMap, typeHints, !outVertexCacheWrite, !inVertexCacheWrite, record.generation);
        } catch (final AerospikeGraphRecordSizeExceededException e) {
            final EdgeRecordSizeExceededException sizeExceededException =
                    fromAddingEdge((AerospikeException) e.getCause(), db, key, edgeId);
            LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    public FireflyEdge writeEdge(final String label,
                                 final List<Map.Entry<String, Object>> properties,
                                 final FireflyVertex inVertex,
                                 final FireflyVertex outVertex) {
        FireflyEdgeId edgeId = (FireflyEdgeId) this.graph.getIdFactory().generateId(this.graph, FireflyEdge.class);
        final FireflyTxn fireflyTxn = getOrCreateFireflyTxn();

        if (this.graph.tx().getCurrentTxn() != null || fireflyTxn != null) {
            try {
                // In Txn mode, write the Edge first so we can handle transaction collisions. Txn also ensures either
                // all or no writes go commit so the ordering doesn't matter.
                // Note: txn being null is allowed. tx() txn is injected at a higher level that overwrites.
                final Txn txn = fireflyTxn == null ? null : fireflyTxn.aerospikeTxn;
                FireflyEdge edge = null;
                while (edge == null) {
                    try {
                        edge = writeEdgeToRecord(edgeId, label, properties, inVertex, outVertex,
                                !inVertex.isEdgeCacheOverflowed(), !outVertex.isEdgeCacheOverflowed(), txn);
                    } catch (final AerospikeGraphException e) {
                        if (e.errorCode == GraphError.RECORD_TX_BLOCKED.code) {
                            this.graph.getIdFactory().recycleEdgeId(edgeId, this.graph, false);
                            edgeId = this.graph.getIdFactory().generateNonRecycledEdgeId(this.graph);
                            LOG.info("A transaction collision occurred when writing a new Edge. Changing target record and retrying.");
                        } else {
                            throw e;
                        }
                    }
                }
                if (this.graph.tx().getCurrentTxn() != null) {
                    // Tinkerpop transaction
                    this.graph.tx().stageUncommittedIdForRecycling(edge.id);
                } else {
                    // MRT transaction
                    fireflyTxn.stageUncommittedIdForRecycling(edge.id);
                }
                writeEdgeToVertex(inVertex, Direction.IN, graph.getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label, txn);
                writeEdgeToVertex(outVertex, Direction.OUT, graph.getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label, txn);

                commit(fireflyTxn);

                if (db.getConfig().isAuditLogEnabled) {
                    // Edge id is byte buffer so not useful.
                    LOG.info("[{}] created edge: [{}]-[{}]>[{}].", graph.getUser(), inVertex.id(), label, outVertex.id());
                }
                return edge;
            } catch (final RuntimeException e) {
                rollback(fireflyTxn);
                throw e;
            }
        } else {
            return writeEdgeWithNoTransaction(edgeId, label, properties, inVertex, outVertex);
        }
    }

    /**
     * Function to write edge to Aerospike with a specified ID. This should not be used by default outside of this
     * class and direct invocation is mostly used for testing.
     *
     * @param edgeId     Edge ID.
     * @param label      Edge label.
     * @param properties Edge properties.
     * @param inVertex   In vertex of edge.
     * @param outVertex  Out vertex of edge.
     * @return Edge.
     */
    public FireflyEdge writeEdgeWithNoTransaction(final FireflyEdgeId edgeId,
                                                  final String label,
                                                  final List<Map.Entry<String, Object>> properties,
                                                  final FireflyVertex inVertex,
                                                  final FireflyVertex outVertex) {
        // Write edge to vertex, if edge write fails, null check on edge record will protect from inconsistent data.
        // Add edge to inVertex and outVertex.
        final boolean inVertexCacheWrite = writeEdgeToVertex(inVertex, Direction.IN, graph.getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label, null);
        final boolean outVertexCacheWrite = writeEdgeToVertex(outVertex, Direction.OUT, graph.getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label, null);

        // Write edge to Aerospike and return FireflyEdge.
        final FireflyEdge edge = writeEdgeToRecord(edgeId, label, properties, inVertex, outVertex, inVertexCacheWrite, outVertexCacheWrite, null);

        if (db.getConfig().isAuditLogEnabled) {
            // Edge id is byte buffer so not useful.
            LOG.info("[{}] created edge: [{}]-[{}]>[{}].", graph.getUser(), inVertex.id(), label, outVertex.id());
        }
        return edge;
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
        LOG.debug("Writing Edge {} to ECACHE of Vertex {} with Direction {}.", edgeId, vertex, direction);
        // Edge cache is overflowed for this vertex - do nothing since writing to overflow bin is on the edge record.
        if (!vertex.writeEdge(direction, edgeId, edgeLabel)) {
            return false;
        }

        // Get bin name for edge direction.
        final String cacheBinName = direction == Direction.IN ? db.getConfig().inEdgesBin : db.getConfig().outEdgesBin;

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.getConfig().vertexAeroSet, vertex.id);

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
                                Exp.boolBin(this.db.getConfig().edgeCacheDisabledBin),
                                Exp.ge(Exp.val(vertex.getEdgeCount(direction)), Exp.val(this.db.getConfig().onRecordIdLimit))
                        ),
                        Exp.val(true),
                        Exp.val(false)
                )
        );
        final Operation updateCacheState = ExpOperation.write(this.db.getConfig().edgeCacheDisabledBin, cacheState, ExpWriteFlags.DEFAULT);
        final Operation getCacheDisabled = Operation.get(this.db.getConfig().edgeCacheDisabledBin);

        // Operate on database.
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.txn = txn;
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record results = this.db.writeOperate(writePolicy, key, appendToEdgeCache, updateCacheState, getCacheDisabled);

            final boolean updatedEdgeCacheOverflowedState =
                    (boolean) OperationReturnHandler.getValueAtIndex(results, this.db.getConfig().edgeCacheDisabledBin, 1);
            // Vertex is promoted to a supernode
            if (updatedEdgeCacheOverflowedState && !vertex.isEdgeCacheOverflowed()) {
                final Txn updaterTxn = txn != null ? txn : graph.tx().getCurrentTxn();
                graph.fireflySummaryUpdater.addSupernodeWriteToQueue(vertex.label(), updaterTxn);
            }
            vertex.setIsEdgeCacheOverflowed(updatedEdgeCacheOverflowedState);
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
        final Key key = new Key(db.getConfig().namespace, db.getConfig().bulkLoadBadEdgeSet, Value.get(id.getStorageId()));
        final Bin addBin = new Bin(db.getConfig().counterBin, count);
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
                                                               final boolean isOutSupernode,
                                                               final boolean isInSupernode,
                                                               final FireflyId outVId, final FireflyId inVId,
                                                               final String label,
                                                               final Map<String, Object> propertyMap) {
        // This method is concurrent traversal safe for removed edges since it's only invoked on creation of a new edge.
        if (!isOutSupernode && !isInSupernode) {
            // No supernodes so we don't have to do anything
            return Collections.emptyList();
        }

        final Value edgeUniqueId = Value.get(edgeId.getUniqueId());
        final Value outVHashIdValue = Value.get(outVId.getKeyHashString());
        final Value inVHashIdValue = Value.get(inVId.getKeyHashString());
        final Value outVUserIdValue = Value.get(outVId.getUserId());
        final Value inVUserIdValue = Value.get(inVId.getUserId());
        final Long schemaLabel = db.schemaManager.getEdgeLabelWrite(label);
        final Long schemaLabelKey = db.schemaManager.getEdgePropertyWrite(EDGE_SUPERNODE_LABEL_KEY);
        final Long schemaAdjacentKey = db.schemaManager.getEdgePropertyWrite(EDGE_SUPERNODE_ADJACENT_ID_KEY);

        final List<Operation> operations = new ArrayList<>();
        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        // Adjacent Vertex ID and Label
        if (isOutSupernode) {
            final String binName = this.db.getConfig().supernodesOutBin;
            final Operation labelOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(schemaLabel),
                    CTX.mapKeyCreate(outVHashIdValue, MapOrder.KEY_ORDERED),
                    CTX.mapKeyCreate(Value.get(schemaLabelKey), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
            final Operation adjacentVOperation = MapOperation.put(policy, binName, edgeUniqueId, inVUserIdValue,
                    CTX.mapKeyCreate(outVHashIdValue, MapOrder.KEY_ORDERED),
                    CTX.mapKeyCreate(Value.get(schemaAdjacentKey), MapOrder.KEY_ORDERED));
            operations.add(adjacentVOperation);
        }
        if (isInSupernode) {
            final String binName = this.db.getConfig().supernodesInBin;
            final Operation labelOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(schemaLabel),
                    CTX.mapKeyCreate(inVHashIdValue, MapOrder.KEY_ORDERED),
                    CTX.mapKeyCreate(Value.get(schemaLabelKey), MapOrder.KEY_ORDERED));
            operations.add(labelOperation);
            final Operation adjacentVOperation = MapOperation.put(policy, binName, edgeUniqueId, outVUserIdValue,
                    CTX.mapKeyCreate(inVHashIdValue, MapOrder.KEY_ORDERED),
                    CTX.mapKeyCreate(Value.get(schemaAdjacentKey), MapOrder.KEY_ORDERED));
            operations.add(adjacentVOperation);
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
        if (!isOutSupernode && !isInSupernode) {
            // No supernodes so we don't have to do anything
            return;
        }
        final Long schemaPropertyKey = db.schemaManager.getEdgePropertyWrite(propertyKey);

        final Value edgeUniqueId = Value.get(edgeId.getUniqueId());
        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);

        if (isOutSupernode) {
            final String binName = this.db.getConfig().supernodesOutBin;
            final Value outVIdValue = Value.get(outVId.getKeyHashString());
            final Operation propertyOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(propertyValue),
                    CTX.mapKey(outVIdValue), CTX.mapKeyCreate(Value.get(schemaPropertyKey), MapOrder.KEY_ORDERED));
            operations.add(propertyOperation);
        }
        if (isInSupernode) {
            final String binName = this.db.getConfig().supernodesInBin;
            final Value inVIdValue = Value.get(inVId.getKeyHashString());
            final Operation propertyOperation = MapOperation.put(policy, binName, edgeUniqueId, Value.get(propertyValue),
                    CTX.mapKey(inVIdValue), CTX.mapKeyCreate(Value.get(schemaPropertyKey), MapOrder.KEY_ORDERED));
            operations.add(propertyOperation);
        }
    }

    private void appendRemoveFilterableSupernodePropertyOperation(final FireflyEdge fireflyEdge,
                                                                  final String propertyKey,
                                                                  final List<Operation> operations) {
        final boolean isOutSupernode = fireflyEdge.isOutSupernode();
        final boolean isInSupernode = fireflyEdge.isInSupernode();
        final FireflyPhatEdgeId edgeId = (FireflyPhatEdgeId) fireflyEdge.id;

        if (!isOutSupernode && !isInSupernode) {
            // No supernodes so we don't have to do anything
            return;
        }

        final Long schemaPropertyKey = db.schemaManager.getEdgePropertyWrite(propertyKey);
        final Exp edgeUniqueId = Exp.val(edgeId.getUniqueId());
        final Value propertyKeyValue = Value.get(schemaPropertyKey);
        final Exp propertyKeyExp = Exp.val(schemaPropertyKey);
        final int writeFlags = ExpWriteFlags.EVAL_NO_FAIL;

        if (isOutSupernode) {
            // Remove Edge ID : Property Value
            final String binName = this.db.getConfig().supernodesOutBin;
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
            final String binName = this.db.getConfig().supernodesInBin;
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
        final Key key = getKey(db, this.db.getConfig().edgeAeroSet, edge.id);
        final long expirationTime = System.currentTimeMillis() + (durationSeconds * 1000);
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation writeTtl = MapOperation.put(policy, db.getConfig().ttlBin, edgeIdMapKey, Value.get(expirationTime));

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
    public void removeEdge(final FireflyEdge edge, final boolean fromOutV, final boolean fromInV, final FireflyTxn outerTxn) {
        final FireflyTxn innerTxn = outerTxn == null ? getOrCreateFireflyTxn() : outerTxn;
        final Txn txn;
        if (innerTxn == null) {
            txn = null;
        } else {
            txn = innerTxn.aerospikeTxn;
        }

        final Key key = getKey(db, db.getConfig().edgeAeroSet, edge.id);
        final List<Operation> operations = new ArrayList<>();
        final Value idKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());

        final Operation removeEdgeData = MapOperation.removeByKey(db.getConfig().edgeDataBin, idKey, MapReturnType.VALUE);
        operations.add(removeEdgeData);
        edge.properties().forEachRemaining(p ->
                appendRemoveFilterableSupernodePropertyOperation(edge, p.key(), operations));

        // Remove adjacent Vertex ID and label pushdowns
        final Exp edgeUniqueId = Exp.val(((FireflyPhatEdgeId) edge.id).getUniqueId());
        final CTX labelMapKeyCtx = CTX.mapKey(Value.get(db.schemaManager.getEdgePropertyWrite(EDGE_SUPERNODE_LABEL_KEY)));
        final CTX adjacentIdMapKeyCtx = CTX.mapKey(Value.get(db.schemaManager.getEdgePropertyWrite(EDGE_SUPERNODE_ADJACENT_ID_KEY)));
        final int writeFlags = ExpWriteFlags.EVAL_NO_FAIL;
        if (edge.isOutSupernode()) {
            final String outVHashId = edge.outVertexId().getKeyHashString();
            final Value outVIdValue = Value.get(outVHashId);
            final Exp outSupernodeBinExp = Exp.mapBin(db.getConfig().supernodesOutBin);
            final Exp outVIdExp = Exp.val(outVHashId);
            final Expression removeEdgeIdToLabelExp = Exp.build(
                    MapExp.removeByKey(edgeUniqueId, outSupernodeBinExp,
                            CTX.mapKey(outVIdValue), labelMapKeyCtx));
            final Operation removeEdgeIdToLabel = ExpOperation.write(db.getConfig().supernodesOutBin,
                    removeEdgeIdToLabelExp, writeFlags);
            operations.add(removeEdgeIdToLabel);
            final Expression removeAdjacentVIdExp = Exp.build(
                    MapExp.removeByKey(edgeUniqueId, outSupernodeBinExp,
                            CTX.mapKey(outVIdValue), adjacentIdMapKeyCtx));
            final Operation removeAdjacentVId = ExpOperation.write(db.getConfig().supernodesOutBin,
                    removeAdjacentVIdExp, writeFlags);
            operations.add(removeAdjacentVId);
            // Remove entire vertex id key if label key is empty since that means there are no items
            final Expression removeVidKeyExp = Exp.build(
                    Exp.cond(
                            Exp.eq(MapExp.size(outSupernodeBinExp, CTX.mapKey(outVIdValue),
                                    labelMapKeyCtx), Exp.val(0)),
                            MapExp.removeByKey(outVIdExp, outSupernodeBinExp),
                            Exp.unknown()
                    )
            );
            final Operation removeVidKey = ExpOperation.write(db.getConfig().supernodesOutBin,
                    removeVidKeyExp, writeFlags);
            operations.add(removeVidKey);
        }
        if (edge.isInSupernode()) {
            final String inVHashId = edge.inVertexId().getKeyHashString();
            final Value inVidValue = Value.get(inVHashId);
            final Exp inSupernodeBinExp = Exp.mapBin(db.getConfig().supernodesInBin);
            final Exp inVidExp = Exp.val(inVHashId);
            final Expression removeEdgeIdToLabelExp = Exp.build(
                    MapExp.removeByKey(edgeUniqueId, inSupernodeBinExp,
                            CTX.mapKey(inVidValue), labelMapKeyCtx));
            final Operation removeEdgeIdToLabel = ExpOperation.write(db.getConfig().supernodesInBin,
                    removeEdgeIdToLabelExp, writeFlags);
            operations.add(removeEdgeIdToLabel);
            final Expression removeAdjacentVIdExp = Exp.build(
                    MapExp.removeByKey(edgeUniqueId, inSupernodeBinExp,
                            CTX.mapKey(inVidValue), adjacentIdMapKeyCtx));
            final Operation removeAdjacentVId = ExpOperation.write(db.getConfig().supernodesInBin,
                    removeAdjacentVIdExp, writeFlags);
            operations.add(removeAdjacentVId);
            // Remove entire vertex id key if label key is empty since that means there are no items
            final Expression removeVidKeyExp = Exp.build(
                    Exp.cond(
                            Exp.eq(MapExp.size(inSupernodeBinExp, CTX.mapKey(inVidValue),
                                    labelMapKeyCtx), Exp.val(0)),
                            MapExp.removeByKey(inVidExp, inSupernodeBinExp),
                            Exp.unknown()
                    )
            );
            final Operation removeVidKey = ExpOperation.write(db.getConfig().supernodesInBin,
                    removeVidKeyExp, writeFlags);
            operations.add(removeVidKey);
        }
        final Operation removeTtl = MapOperation.removeByKey(db.getConfig().ttlBin, idKey, MapReturnType.NONE);
        operations.add(removeTtl);

        // Logic for deleting the entire phat edge record if it no longer contains individual edges.
        final Expression removeEmptyPhatEdgeExp = Exp.build(
                // If the size of the edge data map, which implicitly is the amount of edges in the phat edge, is 0,
                // write null. Otherwise, fail.
                Exp.cond(
                        Exp.eq(MapExp.size(Exp.mapBin(db.getConfig().edgeDataBin)), Exp.val(0)),
                        Exp.nil(),
                        Exp.unknown()
                )
        );
        // If all bins in a record contain null, the record is implicitly deleted.
        final int deletePhatEdgeWriteFlags = ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.ALLOW_DELETE;
        final Operation removeSupernodesInBin = ExpOperation.write(db.getConfig().supernodesInBin, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeSupernodesInBin);
        final Operation removeSupernodesOutBin = ExpOperation.write(db.getConfig().supernodesOutBin, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeSupernodesOutBin);
        final Operation removeTtlBin = ExpOperation.write(db.getConfig().ttlBin, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeTtlBin);

        // This operation must be last since the expression checks the map in the edge data bin.
        final Operation removeEdgeDataBin = ExpOperation.write(db.getConfig().edgeDataBin, removeEmptyPhatEdgeExp, deletePhatEdgeWriteFlags);
        operations.add(removeEdgeDataBin);

        final WritePolicy policy = new WritePolicy();
        policy.txn = txn;
        if (edge.isInSupernode() || edge.isOutSupernode()) {
            policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            policy.generation = edge.getGeneration();
        }

        try {
            final Record record = db.writeOperate(policy, key, ALLOWED_EDGE_DELETE_CODES, false, operations.toArray(new Operation[0]));

            // Result returned is always [List<?>, null] since we have operations [removeEdgeData, removeEdgeDataBin]
            final Command.OpResults results = (Command.OpResults) record.getValue(db.getConfig().edgeDataBin);
            if (results != null && !results.isEmpty()) {
                final Object edgeData = results.get(0);
                // Check edgeData value was returned to protect against concurrent deletes.
                // If this Edge was already removed edgeData returns null and this check returns false.
                if (edgeData instanceof List || edgeData instanceof Map) {
                    if (this.graph.tx().getCurrentTxn() != null) {
                        // Tinkerpop transaction
                        this.graph.tx().stageCommittedIdForRecycling(edge.id);
                        graph.fireflySummaryUpdater.addEdgeRemoveToQueue(edge.label(), this.graph.tx().getCurrentTxn());
                    } else if (innerTxn != null) {
                        // MRT transaction
                        innerTxn.stageCommittedIdForRecycling(edge.id);
                        graph.fireflySummaryUpdater.addEdgeRemoveToQueue(edge.label(), innerTxn.aerospikeTxn);
                    } else {
                        graph.getIdFactory().recycleEdgeId(edge.id, graph, true);
                        graph.fireflySummaryUpdater.addEdgeRemoveToQueue(edge.label(), null);
                    }
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
                    db.rollback(graph, txn);
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
                removeEdge(edges.get(0), false, false, innerTxn);
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

            // commit only own txn
            if (outerTxn == null) {
                commit(innerTxn);
            }
            edge.removed = true;
        } catch (final AerospikeGraphException e) {
            if (outerTxn == null) {
                rollback(innerTxn);
            }
            throw e;
        }

        if (this.db.getConfig().isAuditLogEnabled) {
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
        final String cacheBinName = direction == Direction.IN ? db.getConfig().inEdgesBin : db.getConfig().outEdgesBin;

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.getConfig().vertexAeroSet, vertex.id);

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
        final Operation getCacheDisabled = Operation.get(this.db.getConfig().edgeCacheDisabledBin);

        // Operate on database.
        try {
            WritePolicy policy = new WritePolicy();
            policy.txn = txn;
            final Record results =
                    this.db.writeOperate(policy, key, removeEdgeId, removeEmptyEdgeCacheKeys, getCacheDisabled);

            vertex.setIsEdgeCacheOverflowed(results.getBoolean(this.db.getConfig().edgeCacheDisabledBin));
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
        final Map<FireflyId, FireflyEdgeRecord> edgeRecords = FireflyRecord.batchReadPhatEdges(db, edgeIds);
        if (edgeRecords.isEmpty()) {
            return edges;
        }

        for (final FireflyId edgeId : edgeIds) {
            final FireflyEdgeRecord edgeRecord = edgeRecords.get(edgeId);
            if (edgeRecord != null) {
                final FireflyEdge edge = FireflyEdgeFactory.create((FireflyEdgeId) edgeId, edgeRecord, graph);
                if (edge != null) {
                    edges.add(edge);
                }
            }
        }

        return edges;
    }

    //////////////// EDGE PROPERTIES ///////////////

    public <V> Property<V> writeProperty(final FireflyEdge edge, final String propertyKey, final V value) {
        final Key key = getKey(db, db.getConfig().edgeAeroSet, edge.id);
        final Long schemaPropertyKey = db.schemaManager.getEdgePropertyWrite(propertyKey);
        final boolean isAttachedToSupernode = edge.isInSupernode() || edge.isOutSupernode();
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());
        final Object typeHint = getTypeHintOf(value);
        final MapPolicy typeHintPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);

        final List<Operation> operations = new ArrayList<>();

        if (!isAttachedToSupernode) {
            final MapPolicy propertyPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            final Object propertyValueToWrite = FireflyHelper.convertValueToAerospikeWriteable(value);

            final Operation valueOp = MapOperation.put(propertyPolicy, db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                    Value.get(propertyValueToWrite), CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
            operations.add(valueOp);
            final Operation typeHintOp;
            if (typeHint != null) {
                typeHintOp = MapOperation.put(typeHintPolicy, db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                        Value.get(typeHint), CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
            } else {
                // Remove existing type hint in case there's a previous property with the same key with a type hint
                typeHintOp = MapOperation.removeByKey(db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                        MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
            }
            operations.add(typeHintOp);
        } else {
            if (typeHint != null) {
                final Operation typeHintOp = MapOperation.put(typeHintPolicy, db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                        Value.get(typeHint), CTX.mapKey(edgeIdMapKey));
                operations.add(typeHintOp);
            } else {
                // Workaround since appendFilterableSupernodePropertyOperation is not concurrency-safe.
                // This will cause the entire operation to fail since the edgeIdMapKey does not exist which we want.
                final Operation addDummyTypeHint = MapOperation.put(typeHintPolicy, db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                        Value.get(-1), CTX.mapKey(edgeIdMapKey));
                operations.add(addDummyTypeHint);
                final Operation removeTypeHint = MapOperation.removeByKey(db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                        MapReturnType.NONE, CTX.mapKey(edgeIdMapKey));
                operations.add(removeTypeHint);
            }
            appendFilterableSupernodePropertyOperation((FireflyPhatEdgeId) edge.id, edge.isOutSupernode(),
                    edge.isInSupernode(), edge.outVertexId(), edge.inVertexId(), propertyKey, value, operations);
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));
            edge.addPropertyToCache(propertyKey, value);
            if (typeHint != null) {
                edge.addTypeHint(propertyKey, typeHint);
            }
            graph.fireflySummaryUpdater.addEdgePropertiesWriteToQueue(edge.label(), Set.of(propertyKey), graph.tx().getCurrentTxn());
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
        final boolean isAttachedToSupernode = property.edge().isInSupernode() || property.edge().isOutSupernode();
        final Key key = getKey(db, db.getConfig().edgeAeroSet, property.edge().id);
        final Long schemaPropertyKey = db.schemaManager.getEdgePropertyWrite(property.key());
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) property.edge().id).getEdgeIdBytes());
        final List<Operation> operations = new ArrayList<>();

        if (!isAttachedToSupernode) {
            final Operation removeProperty = MapOperation.removeByKey(db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                    MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
            operations.add(removeProperty);
            final Operation removeTypeHint = MapOperation.removeByKey(db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                    MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
            operations.add(removeTypeHint);
        } else {
            final Operation removeTypeHint = MapOperation.removeByKey(db.getConfig().edgeDataBin, Value.get(schemaPropertyKey),
                    MapReturnType.NONE, CTX.mapKey(edgeIdMapKey));
            operations.add(removeTypeHint);
            appendRemoveFilterableSupernodePropertyOperation(property.edge(), property.key(), operations);
        }

        try {
            property.edge().removePropertyFromCache(property.key());
            db.writeOperate(null, key, operations.toArray(new Operation[0]));
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case the key is
                // the Phat Edge key and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed property {}", property, ae);
            } else {
                throw ae;
            }
        }
    }
}
