package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.ConcurrentScanRecordSequenceListener;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedVertex;
import com.aerospike.firefly.io.utils.GenerationCheck;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
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
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.E_IN_INDEX;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.E_OUT_INDEX;

public abstract class RelationalVertex extends FireflyVertex {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalVertex.class);
    protected final AerospikeConnection db;
    private final Map<String, List<FireflyId>> inEdgeIds;
    private final Map<String, List<FireflyId>> outEdgeIds;
    public final long inEdgeCount;
    public final long outEdgeCount;

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
                               final Map<String, List<FireflyId>> inEdgeIds,
                               final Map<String, List<FireflyId>> outEdgeIds,
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
        // Collect edges in both directions and remove them all.
        final List<FireflyId> edgeIds = getEdgeIdsFromVertex(Direction.BOTH);
        edgeIds.forEach(edgeId -> {
            final FireflyEdge edge = graph.readEdge(FireflyIdFactory.createId(edgeId));
            if (edge != null) {
                edge.remove();
            }
        });

        removeVertexProperties();

        // Remove vertex.
        LOG.debug("Removing vertex {}.", id);
        db.delete(FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, id));

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
    public List<FireflyId> getEdgeIdsFromVertex(final Direction direction) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        if (direction.equals(Direction.OUT)) {
            return getOutEdgeIds();
        } else if (direction.equals(Direction.IN)) {
            return getInEdgeIds();
        } else {
            // Both.
            return getBothEdgeIds();
        }
    }

    /**
     * Helper function for converting a List of edge ids to a List of Vertices.
     *
     * @param edgeIds    List of edge ids.
     * @param direction  Direction.
     * @param edgeLabels Edge labels.
     * @return List of vertices.
     */
    private List<FireflyVertex> verticesFromEdgeIds(final List<FireflyId> edgeIds, final Direction direction, final String... edgeLabels) {
        final List<FireflyRecord> edgeRecords = FireflyRecord.batchRead(db, db.EDGE_AERO_SET, edgeIds);
        if (edgeRecords == null) {
            return new ArrayList<>();
        }
        final List<FireflyEdge> edges = new ArrayList<>();
        for (final FireflyRecord ffr : edgeRecords) {
            if (ffr == null) {
                continue;
            }
            edges.add(RelationalEdge.fromRecord(graph, new KeyRecord(ffr.key(), ffr.record)));
        }
        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        return edges.stream().filter(edge -> edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(edge.label())).
                map(edge -> (FireflyVertex) edge.vertices(direction).next()).
                collect(Collectors.toList());
    }

    /**
     * Get vertices in a specified direction. This function leverages composite ids when appropriate.
     *
     * @param direction  Direction to get edge ids for.
     * @param edgeLabels Edge labels.
     * @return List of vertices.
     */
    @Override
    public List<Vertex> getVerticesFromVertex(final Direction direction, final String... edgeLabels) {
        LOG.trace("Getting vertices from vertex {}.", id);
        final List<Vertex> vertices = new ArrayList<>();
        if (graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY || outEdgeCount == -1 || inEdgeCount == -1) {
            final List<FireflyId> edgeIds = getEdgeIdsFromVertex(direction);
            vertices.addAll(verticesFromEdgeIds(edgeIds, direction, edgeLabels));
        } else {
            final List<FireflyId> vertexIds = new ArrayList<>();
            appendAdjacentVertexIds(vertexIds, direction, edgeLabels);
            List<FireflyRecord> records = FireflyRecord.batchRead(db, db.VERTEX_AERO_SET, vertexIds);
            if (records != null) {
                records.forEach(record -> {
                    if (record != null) {
                        vertices.add(fromRecord(graph, new KeyRecord(record.key(), record.record)));
                    }
                });
            }
        }
        return vertices;
    }

    /**
     * Get incoming edge ids for vertex.
     *
     * @return Iterator of all incoming edge ids.
     */
    private Iterator<FireflyId> getInEdgeIdsIter(final String... labels) {
        if (graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) { //Use index if cache is globally disabled
            return getEdgeIdsFromVertexByIndex(Direction.IN);
        }

        final Set<String> labelList = labels.length == 0 ? new HashSet<>() : Set.of(labels);
        if (inEdgeCount == -1) {
            if (graph.getBaseGraph().ADJACENCY_INDEX_ENABLED) //Use index if available and cache is blown
                return getEdgeIdsFromVertexByIndex(Direction.IN);
            return getEdgeIdsFromVertexByScan(Direction.IN); //Fall back to scan if no index and cache is blown
        } else { //Use cache
            final List<FireflyId> data = new ArrayList<>();
            if (inEdgeIds != null) {
                for (final Map.Entry<String, List<FireflyId>> entry : inEdgeIds.entrySet()) {
                    if (labelList.isEmpty() || labelList.contains(entry.getKey())) {
                        data.addAll(entry.getValue());
                    }
                }
            }
            return data.iterator();
        }
    }

    private List<FireflyId> getInEdgeIds(final String... labels) {
        return IteratorUtils.list(getInEdgeIdsIter(labels));
    }

    /**
     * Get outgoing edge ids for vertex.
     *
     * @return Iterator of all outgoing edge ids.
     */
    private Iterator<FireflyId> getOutEdgeIdsIter() {
        if (graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) { //Use index if cache is globally disabled
            return getEdgeIdsFromVertexByIndex(Direction.OUT);
        }
        if (outEdgeCount == -1) {
            if (graph.getBaseGraph().ADJACENCY_INDEX_ENABLED) //Use index if available and cache is blown
                return getEdgeIdsFromVertexByIndex(Direction.OUT);
            return getEdgeIdsFromVertexByScan(Direction.OUT); //Fall back to scan if no index and cache is blown
        } else { //Use Cache
            final List<FireflyId> data = new ArrayList<>();
            if (outEdgeIds != null) {
                outEdgeIds.values().forEach(data::addAll);
            }
            return data.iterator();
        }
    }

    private List<FireflyId> getOutEdgeIds() {
        return IteratorUtils.list(getOutEdgeIdsIter());
    }

    /**
     * Get incoming and outgoing edge ids for vertex.
     *
     * @return Iterator of all incoming and outgoing edge ids.
     */
    private List<FireflyId> getBothEdgeIds() {
        if (graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) { //Use index if cache is globally disabled
            return IteratorUtils.list(getEdgeIdsFromVertexByIndex(Direction.BOTH));
        }
        return IteratorUtils.list(IteratorUtils.concat(getOutEdgeIdsIter(), getInEdgeIdsIter()));
    }

    /**
     * Get iterator of edge ids from vertex for specified Direction using a scan.
     *
     * @param direction Direction to scan.
     * @return Iterator of edge ids.
     */
    protected Iterator<FireflyId> getEdgeIdsFromVertexByScan(final Direction direction) {
        final Expression exp;
        if (direction == Direction.OUT || direction == Direction.IN) {
            // If direction is in or out, get that specific direction.
            exp = Exp.build(
                    Exp.eq(Exp.intBin(direction == Direction.OUT ? Direction.OUT.name() : Direction.IN.name()),
                            Exp.val((Long) id.getStorageId())
                    ));
        } else {
            // If direction is both, we need to get in and out.
            exp = Exp.build(
                    Exp.or(
                            Exp.eq(Exp.intBin(Direction.IN.name()),
                                    Exp.val((Long) id.getStorageId())
                            ),
                            Exp.eq(Exp.intBin(Direction.OUT.name()),
                                    Exp.val((Long) id.getStorageId())
                            )
                    ));
        }
        // Create scan policy, need bin data for this.
        final ScanPolicy policy = new ScanPolicy();
        policy.includeBinData = true;
        final Iterator<Map.Entry<Key, Record>> i = scanAllRecordsInSet(db.EDGE_AERO_SET, exp, policy);
        return IteratorUtils.map(i, keyRecordEntry -> FireflyIdFactory.createId(keyRecordEntry.getKey().userKey.getObject()));
    }

    protected Iterator<FireflyId> getEdgeIdsFromVertexByIndex(final Direction direction) {
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.sendKey = true;
        queryPolicy.includeBinData = false;

        Iterator<KeyRecord> iterator;
        if (direction == Direction.OUT) {
            iterator = db.queryIndex(db.EDGE_AERO_SET, E_OUT_INDEX, Filter.contains(direction.name(), IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy);
        } else if (direction == Direction.IN) {
            iterator = db.queryIndex(db.EDGE_AERO_SET, E_IN_INDEX, Filter.contains(direction.name(), IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy);
        } else {
            iterator = IteratorUtils.concat(
                    db.queryIndex(db.EDGE_AERO_SET, E_IN_INDEX, Filter.contains(Direction.IN.name(), IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy),
                    db.queryIndex(db.EDGE_AERO_SET, E_OUT_INDEX, Filter.contains(Direction.OUT.name(), IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy));
        }
        return IteratorUtils.map(iterator, keyRecordEntry -> FireflyIdFactory.createId(keyRecordEntry.key.userKey.getObject()));
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
        final Map<String, List<FireflyId>> edgeCache = direction == Direction.IN ? inEdgeIds : outEdgeIds;
        if (edgeCache.containsKey(edgeLabel)) {
            // Needs to be changed to support string (or other) edge ids.
            edgeCache.get(edgeLabel).remove(edgeId);
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
        final Map<String, List<FireflyId>> edgeCache = direction == Direction.IN ? inEdgeIds : outEdgeIds;
        if (!edgeCache.containsKey(edgeLabel)) {
            edgeCache.put(edgeLabel, new ArrayList<>());
        }
        edgeCache.get(edgeLabel).add(edgeId);
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
        GenerationCheck.writeGenerationCheck(() -> protectedRemoveEdge(direction, edgeId, edgeLabel));
    }

    private void protectedRemoveEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        LOG.debug("Removing {} edge {} to vertex {}.", direction, edgeId, id);

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;

        // Get existing firefly record for the vertex.
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, id);
        if (fireflyRecord == null || fireflyRecord.record == null) {
            // Remove edge from local edge cache.
            removeEdgeFromJVMCache(direction, edgeId, edgeLabel);
            return;
        }
        final int generation = fireflyRecord.record.generation;

        // Get label to edge map.
        Map<String, List<Object>> labelEdges = (Map<String, List<Object>>) fireflyRecord.record.getMap(directionKey);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        final Map<String, List<FireflyId>> labelEdgeIds = FireflyIdFactory.convertMapListObjectToFireflyIdMap(labelEdges);

        // Get edge counter for the vertex.
        long edgeCounter = (fireflyRecord.record.getLong(counterKey) - 1);
        if (edgeCounter < 0) {
            edgeCounter = 0;
            LOG.warn("edge counter is 0 for {} when calling removeEdgeFromVertex", this);
        }

        // Get edges.
        final List<FireflyId> edges;
        if (edgeCounter == db.ID_CACHE_SIZE - 1) {
            // If we reach ID_CACHE_SIZE - 1, restore from the cache.
            edges = (direction == Direction.IN) ? getInEdgeIds() : getOutEdgeIds();
        } else {
            // Otherwise grab as normal.
            edges = labelEdgeIds.getOrDefault(edgeLabel, new ArrayList<>());
        }

        // Remove edge from edge list. If we have exceeded the cache, this operation might do nothing.
        edges.remove(edgeId);

        // Update edge list in label to edge map.
        labelEdgeIds.put(edgeLabel, edges);

        // Write label to edge map back to vertex.
        final Bin edgeIdsBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, generation, edgeIdsBin, edgeCounterBin);

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
        GenerationCheck.writeGenerationCheck(() -> protectedWriteEdge(direction, edgeId, edgeLabel));
    }

    private void protectedWriteEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        LOG.debug("Adding {} {} edge {} to vertex {}.", edgeLabel, direction.name(), edgeId, id);

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;

        // Get existing Firefly record for the vertex.
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, id);

        // Initialize edge counter, cache disable flag, edge label map, and generation.
        long edgeCounter = 0;
        boolean cacheDisabled = false;
        Map<String, List<Object>> labelEdges = new HashMap<>();
        int generation = -1;

        // If the Firefly record is not null, grab existing edge data from it.
        if (fireflyRecord != null && fireflyRecord.record != null) {
            labelEdges = (Map<String, List<Object>>) Optional.ofNullable(fireflyRecord.record().getMap(directionKey)).orElse(new HashMap<>());
            edgeCounter = fireflyRecord.record().getLong(counterKey);
            cacheDisabled = fireflyRecord.record().getBoolean(db.CACHE_DISABLED);
            generation = fireflyRecord.record().generation;
        }

        final Bin[] bins;
        edgeCounter++;
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        if (cacheDisabled) {
            // Cache is already disabled.
            bins = new Bin[]{edgeCounterBin};
        } else if (edgeCounter >= db.ID_CACHE_SIZE) {
            // Cache is now disabled due to growing too big.
            final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(true));
            bins = new Bin[]{edgeCounterBin, cacheDisabledBin};
        } else {
            // Add the edge to the cache in the vertex if the cache has not grown too big.
            final List<Object> edges = labelEdges.getOrDefault(edgeLabel, new ArrayList<>());
            edges.add(edgeId.getCachedId());

            // Add edges to edge label map.
            labelEdges.put(edgeLabel, edges);

            // Write edge label map back to vertex.
            final Bin edgeDataBin = new Bin(directionKey, Value.get(labelEdges));
            final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(false));
            bins = new Bin[]{edgeDataBin, edgeCounterBin, cacheDisabledBin};
        }

        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, this.id, generation, bins);

        // Add edge to local edge cache.
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
    public static Map<String, List<FireflyId>> getPropertyIdMap(final FireflyGraph graph,
                                                                final List<Map.Entry<String, Object>> properties,
                                                                final FireflyId vertexId,
                                                                final boolean writeProperties) {
        final AerospikeConnection db = graph.getBaseGraph();

        // Loop through properties and populate the vertex property id cache and vertex property label id map.
        final Map<String, List<FireflyId>> vertexPropertyLabelIdMap = new HashMap<>();
        final Map<String, List<Object>> vertexPropertyValueMap = new HashMap<>();
        properties.forEach(vp -> {
            if (!vertexPropertyValueMap.containsKey(vp.getKey())) {
                vertexPropertyValueMap.put(vp.getKey(), new ArrayList<>());
            }
            vertexPropertyValueMap.get(vp.getKey()).add(vp.getValue());
        });
        vertexPropertyValueMap.forEach((key, value) -> value.forEach(v -> {
                    // Get id for vertex property.
                    final FireflyId vertexPropertyId = FireflyIdFactory.createFromManager(graph, FireflyVertexProperty.class);

                    // Add to vertex property ids to map.
                    if (!vertexPropertyLabelIdMap.containsKey(key)) {
                        vertexPropertyLabelIdMap.put(key, new ArrayList<>());
                    }
                    vertexPropertyLabelIdMap.get(key).add(vertexPropertyId);

                    if (writeProperties) {
                        // Create a bin for the vertex property name (key) and a bin for the vertex property id.
                        final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, key);
                        final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, vertexId.getStorageId());

                        // Write vertex property with type hint.
                        db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vertexPropertyId, db.KEY_VALUE, key, v, vpkBin, pviBin);
                    }
                }
        ));
        return vertexPropertyLabelIdMap;
    }

    static class PropertyValueIdMaps {
        public final Map<String, Object> valueMap;
        public final Map<String, FireflyId> idMap;

        public PropertyValueIdMaps(final Map<String, Object> valueMap, final Map<String, FireflyId> idMap) {
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
        final Map<String, FireflyId> vertexPropertyIdMap = new HashMap<>();
        properties.forEach(vp -> {
            // Get id for vertex property.
            final FireflyId vertexPropertyId = FireflyIdFactory.createFromManager(graph, FireflyVertexProperty.class);

            // Add id and vertex property.
            vertexPropertyValueMap.put(vp.getKey(), vp.getValue());
            vertexPropertyIdMap.put(vp.getKey(), vertexPropertyId);
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
        LOG.debug("Writing Vertex {} {}.", vertexId, properties);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();
        Map<String, ?> vertexPropertyIds;
        Map<String, ?> vertexPropertyIdsWritable;
        final Map<String, Object> vertexPropertyValueMap;
        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                vertexPropertyIds = getPropertyIdMap(graph, properties, vertexId, true);
                vertexPropertyIdsWritable = FireflyIdFactory.convertMapListToStorage((Map<String, List<FireflyId>>) vertexPropertyIds);
                vertexPropertyValueMap = null;
                break;
            case StarPackedVertex.VERTEX_TYPE_HINT:
                // Star specific
                // Fall through
            case PackedVertex.VERTEX_TYPE_HINT:
                final PropertyValueIdMaps propertyValueIdMaps = getPropertyValueIdMaps(graph, properties);
                vertexPropertyIds = propertyValueIdMaps.idMap;
                vertexPropertyIdsWritable = FireflyIdFactory.convertMapToStorage(propertyValueIdMaps.idMap);
                vertexPropertyValueMap = propertyValueIdMaps.valueMap;
                break;
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }

        // Create vertex bins for vertex label, property ids, and property counter.
        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIdsWritable));
        final Bin vertexPropertyCounterBin = new Bin(db.VP_COUNTER, Value.get(Long.valueOf(vertexPropertyIdsWritable.size())));
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

            // Set generation to -1 (no generation check) because this is the initial write of the vertex.
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, -1, labelBin, vertexPropertyIdsBin, vertexPropertyValuesBin, vertexPropertyCounterBin, vertexPropertyValuesTypeHintsBin, typeHint);
        } else {
            vertexPropertyTypeHintMap = null;

            // Set generation to -1 (no generation check) because this is the initial write of the vertex.
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, -1, labelBin, vertexPropertyIdsBin, vertexPropertyCounterBin, typeHint);
        }

        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                return new LinkedVertex(vertexId, label, graph, new HashMap<>(), new HashMap<>(), -1, -1, (Map<String, List<FireflyId>>) vertexPropertyIds, vertexPropertyIds.size(), db);
            case PackedVertex.VERTEX_TYPE_HINT:
                return PackedVertex.PackedVertexFactory.create(vertexId, label, graph, new HashMap<>(), new HashMap<>(),
                        -1, -1, (Map<String, FireflyId>) vertexPropertyIds, vertexPropertyValueMap,
                        vertexPropertyTypeHintMap, vertexPropertyIds.size(), db);
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
        LOG.debug("Reading vertex {}.", vertexId);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();

        // Read the vertex's firefly record from the database
        final FireflyRecord record = FireflyRecord.read(db, db.VERTEX_AERO_SET, vertexId);
        if (record == null) {
            return null;
        }

        return fromRecord(graph, new KeyRecord(record.key(), record.record()));
    }

    /**
     * Read and construct a list of FireflyVertex using the list of FireflyId.
     * This function is static because it is used by the LinkedGraph
     * to write a new FireflyVertex.
     *
     * @param graph     FireflyGraph to use.
     * @param vertexIds FireflyIds to use.
     * @return FireflyVertex.
     */
    public static List<FireflyVertex> readVertices(final FireflyGraph graph, final List<FireflyId> vertexIds) {
        LOG.debug("Reading vertices {}.", vertexIds);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();

        // Batch read vertex records.
        final List<FireflyRecord> vertexRecords = FireflyRecord.batchRead(db, db.VERTEX_AERO_SET, vertexIds);
        if (vertexRecords == null) {
            return new ArrayList<>();
        }

        // Convert records to vertices.
        return vertexRecords.stream().map(record -> fromRecord(graph, new KeyRecord(record.key(), record.record))).
                collect(Collectors.toList());
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
        final FireflyId id = FireflyIdFactory.createFromRecord(db, FireflyRecord.fromRecord(db, keyRecord.key, record));
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
                    return PackedVertex.PackedVertexFactory.create(id, label, graph, new HashMap<>(), new HashMap<>(),
                            -1, -1, new HashMap<>(), new HashMap<>(), new HashMap<>(), vertexPropertyCount,
                            db);
                default:
                    // Should never happen.
                    throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
            }
        }

        // Get incoming and outgoing edge count.
        final long inEdgeCount = record.getLong(db.IN_EDGE_COUNTER);
        final long outEdgeCount = record.getLong(db.OUT_EDGE_COUNTER);

        // Get inEdgeIds and outEdgeIds, if the number of either exceeds the cache size, set to null (invalid).
        final Map<String, List<Object>> inEdgeIds = (inEdgeCount < db.ID_CACHE_SIZE) ?
                (Map<String, List<Object>>) record.getMap(db.IN_EDGES) : new HashMap<>();
        final Map<String, List<Object>> outEdgeIds = (outEdgeCount < db.ID_CACHE_SIZE) ?
                (Map<String, List<Object>>) record.getMap(db.OUT_EDGES) : new HashMap<>();
        final Map<String, List<FireflyId>> fireflyInEdgeIds = FireflyIdFactory.convertMapListObjectToFireflyIdMap(inEdgeIds);
        final Map<String, List<FireflyId>> fireflyOutEdgeIds = FireflyIdFactory.convertMapListObjectToFireflyIdMap(outEdgeIds);
        // Create vertex based on type hint.
        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT: {
                // Get vertex properties and vertex property counter from record.
                final Map<String, List<Object>> vertexProperties = (vertexPropertyCount < db.ID_CACHE_SIZE) ?
                        (Map<String, List<Object>>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID) : new HashMap<>();
                final Map<String, List<FireflyId>> fireflyVertexProperties = FireflyIdFactory.convertMapListObjectToFireflyIdMap(vertexProperties);
                return new LinkedVertex(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds, inEdgeCount, outEdgeCount, fireflyVertexProperties, vertexPropertyCount, db);
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
                final Map<String, Object> vertexPropertyIds = (vertexPropertyCount < db.ID_CACHE_SIZE) ?
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID) : new HashMap<>();
                final Map<String, FireflyId> fireflyVertexPropertyIds = FireflyIdFactory.convertMapObjectToFireflyIdMap(vertexPropertyIds);
                return PackedVertex.PackedVertexFactory.create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds, inEdgeCount,
                        outEdgeCount, fireflyVertexPropertyIds, vertexPropertyValues, vertexPropertyTypeHints,
                        vertexPropertyCount, db);
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }
    }

    /**
     * Used by strategies to leverage composite ids to get adjacent vertex ids.
     *
     * @param adjacentVertexIds List of adjacent vertex ids to append to.
     * @param direction         Direction of edges to get adjacent vertex ids for.
     * @param edgeLabels        Labels of edges to filter with.
     * @return how many vertices were added.
     */
    public int appendAdjacentVertexIds(final List<FireflyId> adjacentVertexIds, final Direction direction, final String... edgeLabels) {
        int i = 0;
        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (this.inEdgeCount == -1) {
                // Should not happen, this is checked before function is called.
                throw new RuntimeException("Error, cannot use appendAdjacentVertexIds unless vertices are cached.");
            }
            for (final Map.Entry<String, List<FireflyId>> entry : this.inEdgeIds.entrySet()) {
                if (edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(entry.getKey())) {
                    for (FireflyId edgeId : entry.getValue()) {
                        adjacentVertexIds.add(((FireflyIdComposite) edgeId).getAdjacentId());
                        i++;
                    }
                }
            }
        }
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            if (this.outEdgeCount == -1) {
                // Should not happen, this is checked before function is called.
                throw new RuntimeException("Error, cannot use appendAdjacentVertexIds unless vertices are cached.");
            }
            for (final Map.Entry<String, List<FireflyId>> entry : this.outEdgeIds.entrySet()) {
                if (edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(entry.getKey())) {
                    for (FireflyId edgeId : entry.getValue()) {
                        adjacentVertexIds.add(((FireflyIdComposite) edgeId).getAdjacentId());
                        i++;
                    }
                }
            }
        }
        return i;
    }
}
