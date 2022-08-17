package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.ConcurrentScanRecordSequenceListener;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class RelationalVertex extends FireflyVertex {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalVertex.class);
    private static final int THREAD_COUNT = 2;

    // TODO (https://aerospike.atlassian.net/browse/GRAPH-90)
    //  Switch to a single thread with aggregate ids in a bulk request
    //  as opposed to 2 requests running in parallel.
    private static final ExecutorService executorService = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    private AerospikeConnection db;
    private Map<String, List<Long>> inEdgeIds;
    private Map<String, List<Long>> outEdgeIds;
    private long inEdgeCount;
    private long outEdgeCount;

    /**
     * Constructor for RelationalVertex.
     *
     * @param fid          firefly id.
     * @param label        label.
     * @param graph        graph.
     * @param inEdgeIds    incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds   outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount  incoming edge count.
     * @param outEdgeCount outgoing edge count.
     * @param db           Aerospike connection.
     */
    protected RelationalVertex(final FireflyId fid,
                               final String label,
                               final FireflyGraph graph,
                               final Map<String, List<Long>> inEdgeIds,
                               final Map<String, List<Long>> outEdgeIds,
                               final long inEdgeCount,
                               final long outEdgeCount,
                               final AerospikeConnection db) {
        super(fid, label, graph);
        this.inEdgeIds = inEdgeIds == null ? new HashMap<>() : inEdgeIds;
        this.outEdgeIds = outEdgeIds == null ? new HashMap<>() : outEdgeIds;
        this.inEdgeCount = inEdgeCount;
        this.outEdgeCount = outEdgeCount;
        this.db = db;
    }

    protected abstract void removeVertexProperties();

    /**
     * Remove vertex. Any edges attached to a vertex must be removed
     * when the edge is removed.
     */
    @Override
    public void remove() {
        // Collect edges in both directions.
        final Iterator<Long> inEdgeIds = getEdgeIdsFromVertex(Direction.IN);
        final Iterator<Long> outEdgeIds = getEdgeIdsFromVertex(Direction.OUT);

        // Take list of ids and convert to a set so we can remove duplicates.
        final Set<Long> inEdgeIdSet = new HashSet<>();
        final Set<Long> outEdgeIdSet = new HashSet<>();
        while (inEdgeIds.hasNext()) {
            inEdgeIdSet.add(inEdgeIds.next());
        }
        while (outEdgeIds.hasNext()) {
            outEdgeIdSet.add(outEdgeIds.next());
        }

        // Remove edge. Note, using removeEdge() function because it negates trying to remove the edge from the vertex.
        // Also remove edge from vertex.
        inEdgeIdSet.forEach(edgeId -> {
            final FireflyEdge edge = graph.readEdge(FireflyId.of(FireflyEdge.class, edgeId));
            if (edge != null) {
                edge.removeEdge();
                final RelationalVertex vertex = (RelationalVertex) edge.outVertex();
                if (vertex != null)
                    vertex.removeEdge(Direction.OUT, FireflyId.of(FireflyEdge.class, edgeId), edge.label());
            }
        });
        outEdgeIdSet.forEach(edgeId -> {
            final FireflyEdge edge = graph.readEdge(FireflyId.of(FireflyEdge.class, edgeId));
            if (edge != null) {
                edge.removeEdge();
                final RelationalVertex vertex = (RelationalVertex) edge.inVertex();
                if (vertex != null)
                    vertex.removeEdge(Direction.IN, FireflyId.of(FireflyEdge.class, edgeId), edge.label());
            }
        });

        removeVertexProperties();

        // Remove vertex.
        LOG.debug("Removing vertex {}.", id.value().toString());
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, id.toNumericId()));

        // Set flags to indicate vertex has been removed.
        this.removed = true;
    }

    /**
     * Get edge ids from vertex for given Direction.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    @Override
    public Iterator<Long> getEdgeIdsFromVertex(final Direction direction) {
        LOG.trace("Getting edge ids from vertex {}.", id.value().toString());
        if (direction.equals(Direction.OUT)) {
            return getOutEdgeIds();
        } else if (direction.equals(Direction.IN)) {
            return getInEdgeIds();
        } else {
            // Both.
            final Future<Iterator<Long>> inIds = executorService.submit(this::getInEdgeIds);
            final Future<Iterator<Long>> outIds = executorService.submit(this::getOutEdgeIds);
            try {
                return IteratorUtils.concat(inIds.get(), outIds.get());
            } catch (InterruptedException | ExecutionException e) {
                // Should not happen.
                LOG.error("Error getting edge ids from vertex {} {}.", id.value().toString(), e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get incoming edge id for vertex.
     *
     * @return Iterator of all incoming edge ids.
     */
    private Iterator<Long> getInEdgeIds() {
        final List<Long> data = new ArrayList<>();
        if (inEdgeIds != null) {
            inEdgeIds.values().forEach(data::addAll);
        }
        return (inEdgeCount != -1) ? data.iterator() : getEdgeIdsFromVertexByScan(Direction.IN);
    }

    /**
     * Get outgoing edge id for vertex.
     *
     * @return Iterator of all outgoing edge ids.
     */
    private Iterator<Long> getOutEdgeIds() {
        final List<Long> data = new ArrayList<>();
        if (outEdgeIds != null) {
            outEdgeIds.values().forEach(data::addAll);
        }
        return (outEdgeCount != -1) ? data.iterator() : getEdgeIdsFromVertexByScan(Direction.OUT);
    }

    /**
     * Get iterator of edge ids from vertex for specified Direction using a scan.
     *
     * @param direction Direction to scan.
     * @return Iterator of edge ids.
     */
    private Iterator<Long> getEdgeIdsFromVertexByScan(final Direction direction) {
        if (direction == Direction.OUT || direction == Direction.IN) {
            // If direction is in or out, get that specific direction.
            final Expression exp = Exp.build(
                    Exp.eq(Exp.intBin(direction == Direction.OUT ? Direction.OUT.name() : Direction.IN.name()),
                            Exp.val((Long) db.idToStorageType(this.id()))
                    ));

            final ScanPolicy policy = new ScanPolicy();
            policy.includeBinData = false;
            final Iterator<Map.Entry<Key, Record>> i = scanAllRecordsInSet(db.EDGE_AERO_SET, exp, policy);
            return IteratorUtils.map(i, keyRecordEntry -> (Long) keyRecordEntry.getKey().userKey.getObject());
        } else {
            // If direction is both, we need to get in and out.
            final Expression inExpression = Exp.build(
                    Exp.eq(Exp.intBin(Direction.IN.name()),
                            Exp.val((Long) db.idToStorageType(this.id()))
                    ));
            final Expression outExpression = Exp.build(
                    Exp.eq(Exp.intBin(Direction.OUT.name()),
                            Exp.val((Long) db.idToStorageType(this.id()))
                    ));

            // Create scan policy, do not need bin data for this.
            final ScanPolicy policy = new ScanPolicy();
            policy.includeBinData = false;

            // Run in and out scans in parallel.
            final Future<Iterator<Map.Entry<Key, Record>>> inIterator = executorService.submit(() -> scanAllRecordsInSet(db.EDGE_AERO_SET, inExpression, policy));
            final Future<Iterator<Map.Entry<Key, Record>>> outIterator = executorService.submit(() -> scanAllRecordsInSet(db.EDGE_AERO_SET, outExpression, policy));

            // Concatenate the in and out iterators.
            try {
                return IteratorUtils.concat(IteratorUtils.map(outIterator.get(), keyRecordEntry -> (Long) keyRecordEntry.getKey().userKey.getObject()),
                        IteratorUtils.map(inIterator.get(), keyRecordEntry -> (Long) keyRecordEntry.getKey().userKey.getObject()));
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error getting edge ids from vertex {} by scan {}.", id.value().toString(), e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Issue a scan query for all the records in the edge set.
     * Filter by an Exp, provide a ScanPolicy
     * <p>
     * Note - if the client or the event loop was closed prior to this, this function will hang indefinitely.
     *
     * @param exp    Expression to apply to Scan
     * @param policy ScanPolicy to use during Scan
     * @return Iterator of Map.Entry Key, Record
     */
    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String set, final Expression exp, final ScanPolicy policy) {
        LOG.trace("Issuing scan query of all records in {}:{} with filter {}.",
                db.getNamespace(), set, exp);
        final Monitor scanMonitor = new Monitor();
        policy.sendKey = true;
        if (exp != null)
            policy.filterExp = exp;
        final AerospikeClient client = db.getClient();
        final ConcurrentScanRecordSequenceListener listener = new ConcurrentScanRecordSequenceListener(
                scanMonitor,
                Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.SCAN_MAX_WAIT, db.conf)));
        client.scanAll(db.getEventLoops().next(), listener, policy, db.getNamespace(), set);
        return listener.iterator();
    }

    /**
     * Remove edge from vertex property JVM cache (cache inside this vertex object).
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    private void removeEdgeFromJVMCache(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        final Map<String, List<Long>> edgeCache = direction == Direction.IN ? inEdgeIds : outEdgeIds;
        if (edgeCache.containsKey(edgeLabel)) {
            edgeCache.get(edgeLabel).remove((Long) edgeId.toNumericId().value());
        }
    }

    /**
     * Add edge to vertex property JVM cache (cache inside this vertex object).
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    private void addEdgeToJVMCache(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        final Map<String, List<Long>> edgeCache = direction == Direction.IN ? inEdgeIds : outEdgeIds;
        if (!edgeCache.containsKey(edgeLabel)) {
            edgeCache.put(edgeLabel, new ArrayList<>());
        }
        edgeCache.get(edgeLabel).add((Long) edgeId.toNumericId().value());
    }

    /**
     * Remove edge from vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    @Override
    protected void removeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        LOG.debug("Removing edge {} to vertex {}.", edgeId.value(), id.value());

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;

        // Get existing firefly record for the vertex.
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, id.toNumericId());
        if (fireflyRecord == null || fireflyRecord.record == null) {
            // Remove edge from local edge cache.
            removeEdgeFromJVMCache(direction, edgeId, edgeLabel);
            return;
        }

        // Get label to edge map.
        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) fireflyRecord.record.getMap(directionKey);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }

        // Get edge counter for the vertex.
        long edgeCounter = (fireflyRecord.record.getLong(counterKey) - 1);
        if (edgeCounter < 0) {
            edgeCounter = 0;
            LOG.warn("edge counter is 0 for {} when calling removeEdgeFromVertex", this);
        }

        // Get edges.
        final List<Long> edges;
        if (edgeCounter == db.ID_CACHE_SIZE - 1) {
            // If we reach ID_CACHE_SIZE - 1, restore from the cache.
            final Iterator<Long> allEdges = (direction == Direction.IN) ? getInEdgeIds() : getOutEdgeIds();
            edges = new ArrayList<>();
            while (allEdges.hasNext()) {
                edges.add(allEdges.next());
            }
        } else {
            // Otherwise grab as normal.
            edges = labelEdges.getOrDefault(edgeLabel, new ArrayList<>());
        }

        // Remove edge from edge list. If we have exceeded the cache, this operation might do nothing.
        edges.remove(NumericIdManager.convert(edgeId));

        // Update edge list in label to edge map.
        labelEdges.put(edgeLabel, edges);

        // Write label to edge map back to vertex.
        final Bin edgeIdsBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, edgeIdsBin, edgeCounterBin);

        // Remove edge from local edge cache.
        removeEdgeFromJVMCache(direction, edgeId, edgeLabel);
    }

    /**
     * Write edge to vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    @Override
    public void writeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        LOG.debug("Adding {} {} edge {} to vertex {}.", edgeLabel, direction.name(), edgeId.value(), id.value());

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;

        // Get existing firefly record for the vertex.
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, id.toNumericId());

        // Initialize edge counter, cache disable flag, and edge label map.
        long edgeCounter = 0;
        boolean cacheDisabled = false;
        Map<String, List<Long>> labelEdges = new HashMap<>();

        // If the firefly record is not null, grab existing the edge metadata from it.
        if (fireflyRecord != null && fireflyRecord.record() != null) {
            labelEdges = (Map<String, List<Long>>) Optional.ofNullable(fireflyRecord.record().getMap(directionKey)).orElse(new HashMap<>());
            edgeCounter = fireflyRecord.record().getLong(counterKey);
            cacheDisabled = fireflyRecord.record().getBoolean(db.CACHE_DISABLED);
        }

        // Add the edge to the cache in the vertex if the cache has not grown too big.
        final List<Long> edges = labelEdges.getOrDefault(edgeLabel, new ArrayList<>());
        if (edgeCounter < db.ID_CACHE_SIZE)
            edges.add(NumericIdManager.convert(edgeId.value()));
        else
            cacheDisabled = true;
        edgeCounter++;

        // Add edges to edge label map.
        labelEdges.put(edgeLabel, edges);

        // Write label to edge map back to vertex.
        final Bin edgeDataBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(cacheDisabled));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, edgeDataBin, edgeCounterBin, cacheDisabledBin);

        // Remove edge from local edge cache.
        addEdgeToJVMCache(direction, edgeId, edgeLabel);
    }

    /**
     * Get property id map.
     *
     * @param graph           Graph to use.
     * @param properties      Properties.
     * @param vertexId        Vertex id.
     * @param writeProperties Write properties flag.
     * @return Property id map.
     */
    public static Map<String, List<Long>> getPropertyIdMap(final FireflyGraph graph,
                                                           final List<Map.Entry<String, Object>> properties,
                                                           final FireflyId vertexId,
                                                           final boolean writeProperties) {
        final AerospikeConnection db = graph.getBaseGraph();

        // Loop through properties and populate the vertex property id cache and vertex property label id map.
        final Map<String, List<Long>> vertexPropertyLabelIdMap = new HashMap<>();
        final Map<String, List<Object>> vertexPropertyValueMap = new HashMap<>();
        properties.forEach(vp -> {
            if (!vertexPropertyValueMap.containsKey(vp.getKey())) {
                vertexPropertyValueMap.put(vp.getKey(), new ArrayList<>());
            }
            vertexPropertyValueMap.get(vp.getKey()).add(vp.getValue());
        });
        vertexPropertyValueMap.forEach((key, value) -> value.forEach(v -> {
                    // Get id for vertex property.
                    final FireflyId vertexPropertyId = FireflyId.createFromManager(graph, FireflyVertexProperty.class);

                    // Add to vertex property ids to map.
                    if (!vertexPropertyLabelIdMap.containsKey(key)) {
                        vertexPropertyLabelIdMap.put(key, new ArrayList<>());
                    }
                    vertexPropertyLabelIdMap.get(key).add(NumericIdManager.convert(vertexPropertyId.value()));

                    if (writeProperties) {
                        // Create a bin for the vertex property name (key) and a bin for the vertex property id.
                        final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, key);
                        final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, AerospikeConnection.idToStorageType(vertexId.value()));

                        // Write vertex property with type hint.
                        db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vertexPropertyId, db.KEY_VALUE, key, v, vpkBin, pviBin);
                    }
                }
        ));
        return vertexPropertyLabelIdMap;
    }

    static class PropertyValueIdMaps {
        public final Map<String, Object> valueMap;
        public final Map<String, Long> idMap;

        public PropertyValueIdMaps(final Map<String, Object> valueMap, final Map<String, Long> idMap) {
            this.valueMap = valueMap;
            this.idMap = idMap;
        }
    }

    /**
     * Get property value map.
     *
     * @param graph      Graph to use.
     * @param properties Properties.
     * @return Property value map.
     */
    public static PropertyValueIdMaps getPropertyValueIdMaps(final FireflyGraph graph, final List<Map.Entry<String, Object>> properties) {
        // Loop through properties nad populate the vertex properties value map.
        final Map<String, Object> vertexPropertyValueMap = new HashMap<>();
        final Map<String, Long> vertexPropertyIdMap = new HashMap<>();
        properties.forEach(vp -> {
            // Get id for vertex property.
            final FireflyId vertexPropertyId = FireflyId.createFromManager(graph, FireflyVertexProperty.class);

            // Add id and vertex property.
            vertexPropertyValueMap.put(vp.getKey(), vp.getValue());
            vertexPropertyIdMap.put(vp.getKey(), (Long) vertexPropertyId.toNumericId().value());
        });

        // Return vertex property value map.
        return new PropertyValueIdMaps(vertexPropertyValueMap, vertexPropertyIdMap);
    }

    /**
     * Write and construct a FireflyVertex using the provided parameters.
     * This function is static because it is used by the RelationalGraph
     * to write a new FireflyVertex.
     *
     * @param graph      FireflyGraph to use.
     * @param vertexId   id of vertex,.
     * @param label      String label of vertex.
     * @param properties Map of properties to add to vertex.
     * @return FireflyVertex.
     */
    public static FireflyVertex writeVertex(final FireflyGraph graph,
                                            final FireflyId vertexId,
                                            final String label,
                                            final List<Map.Entry<String, Object>> properties,
                                            final int vertexTypeHint) {
        LOG.debug("Writing Vertex {} {}.", vertexId.value().toString(), properties);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();
        final Map<String, ?> vertexPropertyIds;
        final Map<String, Object> vertexPropertyValueMap;
        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                vertexPropertyIds = getPropertyIdMap(graph, properties, vertexId, true);
                vertexPropertyValueMap = null;
                break;
            case PackedVertex.VERTEX_TYPE_HINT:
                final PropertyValueIdMaps propertyValueIdMaps = getPropertyValueIdMaps(graph, properties);
                vertexPropertyIds = propertyValueIdMaps.idMap;
                vertexPropertyValueMap = propertyValueIdMaps.valueMap;
                break;
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }

        // Create vertex bins for vertex label, property ids, and property counter.
        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIds));
        final Bin vertexPropertyCounterBin = new Bin(db.VP_COUNTER, Value.get(Long.valueOf(vertexPropertyIds.size())));
        final Bin typeHint = new Bin(db.RELATIONAL_VERTEX_TYPE_HINT, Value.get(vertexTypeHint));

        // Write vertex bins to Aerospike.
        final Map<String, Long> vertexPropertyTypeHintMap;
        if (vertexPropertyValueMap != null) {
            final Bin vertexPropertyValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexPropertyValueMap));
            vertexPropertyTypeHintMap = new HashMap<>();
            for (Map.Entry<String, ?> entry : vertexPropertyValueMap.entrySet()) {
                vertexPropertyTypeHintMap.put(entry.getKey(), db.getSupportedType(entry.getValue().getClass()));
            }
            final Bin vertexPropertyValuesTypeHintsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexPropertyTypeHintMap));
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, labelBin, vertexPropertyIdsBin, vertexPropertyValuesBin, vertexPropertyCounterBin, vertexPropertyValuesTypeHintsBin, typeHint);
        } else {
            vertexPropertyTypeHintMap = null;
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, labelBin, vertexPropertyIdsBin, vertexPropertyCounterBin, typeHint);
        }

        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                return new LinkedVertex(vertexId, label, graph, new HashMap<>(), new HashMap<>(), -1, -1, (Map<String, List<Long>>) vertexPropertyIds, vertexPropertyIds.size(), db);
            case PackedVertex.VERTEX_TYPE_HINT:
                return new PackedVertex(vertexId, label, graph, new HashMap<>(), new HashMap<>(), -1, -1, (Map<String, Long>) vertexPropertyIds, vertexPropertyValueMap, vertexPropertyTypeHintMap, vertexPropertyIds.size(), db);
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }
    }

    /**
     * Read and construct a FireflyVertex using the FireflyId.
     * This function is static because it is used by the LinkedGraph
     * to write a new FireflyVertex.
     *
     * @param graph    FireflyGraph to use.
     * @param vertexId FireflyId to use.
     * @return FireflyVertex.
     */
    public static FireflyVertex readVertex(final FireflyGraph graph, final FireflyId vertexId) {
        LOG.debug("Reading Vertex {}.", vertexId.value().toString());

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();

        // Read the vertex's firefly record from the database
        final FireflyRecord record = FireflyRecord.read(db, db.VERTEX_AERO_SET, vertexId.toNumericId());
        if (record == null) {
            return null;
        }

        return fromRecord(graph, new KeyRecord(record.key(), record.record()));
    }

    /**
     * Construct vertex from Record.
     *
     * @param graph     FireflyGraph to use.
     * @param keyRecord Record to construct vertex with.
     * @return FireflyVertex.
     */
    public static FireflyVertex fromRecord(final FireflyGraph graph, final KeyRecord keyRecord) {
        if (keyRecord == null) {
            return null;
        }

        final Record record = keyRecord.record;

        // Read the vertex's firefly record from the database
        if (record == null) {
            return null;
        }
        final AerospikeConnection db = graph.getBaseGraph();

        // Get id and label for vertex.
        final FireflyId id = FireflyId.loadFromAerospike(db, FireflyVertex.class, FireflyRecord.fromRecord(db, keyRecord.key, record));
        final int vertexTypeHint = record.getInt(db.RELATIONAL_VERTEX_TYPE_HINT);
        final String label = record.getString(AerospikeConnection.LABEL);

        // If cache is disabled, inEdgeIds and outEdgeIds are null.
        final boolean cacheDisabled = record.getBoolean(db.CACHE_DISABLED);
        final long vertexPropertyCount = record.getLong(db.VP_COUNTER);
        if (cacheDisabled) {
            // Set inEdgeIds and outEdgeIds to null (invalid).
            switch (vertexTypeHint) {
                case LinkedVertex.VERTEX_TYPE_HINT:
                    return new LinkedVertex(id, label, graph, new HashMap<>(), new HashMap<>(), -1, -1, new HashMap<>(), vertexPropertyCount, db);
                case PackedVertex.VERTEX_TYPE_HINT:
                    return new PackedVertex(id, label, graph, new HashMap<>(), new HashMap<>(), -1, -1, new HashMap<>(), new HashMap<>(), new HashMap<>(), vertexPropertyCount, db);
                default:
                    // Should never happen.
                    throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
            }
        }

        // Get incoming and outgoing edge count.
        final long inEdgeCount = record.getLong(db.IN_EDGE_COUNTER);
        final long outEdgeCount = record.getLong(db.OUT_EDGE_COUNTER);

        // Get inEdgeIds and outEdgeIds, if the number of either exceeds the cache size, set to null (invalid).
        final Map<String, List<Long>> inEdgeIds = (inEdgeCount < db.ID_CACHE_SIZE) ?
                (Map<String, List<Long>>) record.getMap(db.IN_EDGES) : new HashMap<>();
        final Map<String, List<Long>> outEdgeIds = (outEdgeCount < db.ID_CACHE_SIZE) ?
                (Map<String, List<Long>>) record.getMap(db.OUT_EDGES) : new HashMap<>();

        // Create vertex based on type hint.
        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT: {
                // Get vertex properties and vertex property counter from record.
                final Map<String, List<Long>> vertexProperties = (vertexPropertyCount < db.ID_CACHE_SIZE) ?
                        (Map<String, List<Long>>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID) : new HashMap<>();
                return new LinkedVertex(id, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, vertexProperties, vertexPropertyCount, db);
            }
            case PackedVertex.VERTEX_TYPE_HINT:
                // Get vertex properties and vertex property counter from record.
                final Map<String, Object> vertexPropertyValues = (vertexPropertyCount < db.ID_CACHE_SIZE) ?
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE) : new HashMap<>();
                final Map<String, Long> vertexPropertyTypeHints = (vertexPropertyCount < db.ID_CACHE_SIZE) ?
                        (Map<String, Long>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT) : new HashMap<>();
                for (String key : vertexPropertyValues.keySet()) {
                    vertexPropertyValues.put(key, db.convertValuetoTypeUsingHint(vertexPropertyValues.get(key), vertexPropertyTypeHints.get(key)));
                }
                final Map<String, Long> vertexPropertyIds = (vertexPropertyCount < db.ID_CACHE_SIZE) ?
                        (Map<String, Long>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID) : new HashMap<>();
                return new PackedVertex(id, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, vertexPropertyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyCount, db);
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }
    }
}
