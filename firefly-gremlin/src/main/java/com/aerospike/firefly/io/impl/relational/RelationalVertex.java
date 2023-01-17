package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.ConcurrentScanRecordSequenceListener;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedVertex;
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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.utils.GenerationCheck.RECORD_TOO_BIG_ERROR;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.E_IN_INDEX;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.E_OUT_INDEX;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public abstract class RelationalVertex extends FireflyVertex {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalVertex.class);
    private final Map<String, List<FireflyId>> inEdgeIds;
    private final Map<String, List<FireflyId>> outEdgeIds;
    private long inEdgeCount;
    private long outEdgeCount;
    protected boolean isEdgeCacheDisabled;
    protected final AerospikeConnection db;

    /**
     * Constructor for RelationalVertex.
     *
     * @param fid                 firefly id.
     * @param label               label.
     * @param graph               graph.
     * @param inEdgeIds           incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds          outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount         incoming edge count.
     * @param outEdgeCount        outgoing edge count.
     * @param isEdgeCacheDisabled is edge cache disabled.
     * @param db                  Aerospike connection.
     */
    protected RelationalVertex(final FireflyId fid,
                               final String label,
                               final FireflyGraph graph,
                               final Map<String, List<FireflyId>> inEdgeIds,
                               final Map<String, List<FireflyId>> outEdgeIds,
                               final long inEdgeCount,
                               final long outEdgeCount,
                               final boolean isEdgeCacheDisabled,
                               final AerospikeConnection db) {
        super(fid, label, graph);
        this.inEdgeIds = inEdgeIds == null ? new TreeMap<>() : inEdgeIds;
        this.outEdgeIds = outEdgeIds == null ? new TreeMap<>() : outEdgeIds;
        this.inEdgeCount = inEdgeCount;
        this.outEdgeCount = outEdgeCount;
        this.isEdgeCacheDisabled = isEdgeCacheDisabled;
        this.db = db;
    }

    protected abstract void removeVertexProperties();

    /**
     * Remove vertex. Any edges attached to adjacent vertices must be removed
     * from the adjacent vertices when the edge is removed.
     */
    @Override
    public void remove() {
        // Collect edges in both directions and remove them all.
        final Set<FireflyId> edgeIds = new HashSet<>(getEdgeIdsFromVertex(Direction.BOTH));
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
    private List<Vertex> verticesFromEdgeIds(final List<FireflyId> edgeIds, final Direction direction,
                                             final String... edgeLabels) {
        final List<FireflyRecord> edgeRecords = FireflyRecord.batchRead(db, db.EDGE_AERO_SET, edgeIds);
        if (edgeRecords == null)
            return new ArrayList<>();

        final List<FireflyEdge> edges = new ArrayList<>();
        for (final FireflyRecord ffr : edgeRecords) {
            if (ffr == null)
                continue;
            edges.add(RelationalEdge.fromRecord(graph, new KeyRecord(ffr.key(), ffr.record)));
        }
        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        final List<Vertex> listOfEdges = edges.stream()
                .filter(edge -> edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(edge.label()))
                .flatMap(edge -> {
                    if (id().equals(edge.outVertex().id()) && id().equals(edge.inVertex().id()))
                        return Stream.of(this);
                    return IteratorUtils.stream(IteratorUtils.filter(edge.vertices(direction.opposite()),
                            vertex -> !vertex.id().equals(id())));
                }).collect(Collectors.toList());
        return listOfEdges;
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
        if (this.isEdgeCacheDisabled) {
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
    private Iterator<FireflyId> getInEdgeIdsIter() {
        if (!this.isEdgeCacheDisabled) { // Use cache
            final List<FireflyId> data = new ArrayList<>();
            if (inEdgeIds != null) {
                inEdgeIds.values().forEach(data::addAll);
            }
            return data.iterator();
        } else if (graph.getBaseGraph().ADJACENCY_INDEX_ENABLED) { // Use index if available and cache is blown
            return getEdgeIdsFromVertexByIndex(Direction.IN);
        } else { // Fall back to scan if no index and cache is blown
            return getEdgeIdsFromVertexByScan(Direction.IN);
        }
    }

    private List<FireflyId> getInEdgeIds() {
        return IteratorUtils.list(getInEdgeIdsIter());
    }

    /**
     * Get outgoing edge ids for vertex.
     *
     * @return Iterator of all outgoing edge ids.
     */
    private Iterator<FireflyId> getOutEdgeIdsIter() {
        if (!this.isEdgeCacheDisabled) { // Use cache
            final List<FireflyId> data = new ArrayList<>();
            if (outEdgeIds != null) {
                outEdgeIds.values().forEach(data::addAll);
            }
            return data.iterator();
        } else if (graph.getBaseGraph().ADJACENCY_INDEX_ENABLED) { // Use index if available and cache is blown
            return getEdgeIdsFromVertexByIndex(Direction.OUT);
        } else {
            return getEdgeIdsFromVertexByScan(Direction.OUT); // Fall back to scan if no index and cache is blown
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
        return IteratorUtils.map(i, keyRecordEntry ->
                FireflyIdFactory.createId(keyRecordEntry.getKey().userKey.getObject()));
    }

    protected Iterator<FireflyId> getEdgeIdsFromVertexByIndex(final Direction direction) {
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.sendKey = true;
        queryPolicy.includeBinData = false;
        Iterator<KeyRecord> iterator;
        if (direction == Direction.OUT) {
            iterator = db.queryIndex(db.EDGE_AERO_SET, E_OUT_INDEX, Filter.contains(direction.name(),
                    IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy);
        } else if (direction == Direction.IN) {
            iterator = db.queryIndex(db.EDGE_AERO_SET, E_IN_INDEX, Filter.contains(direction.name(),
                    IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy);
        } else {
            iterator = IteratorUtils.concat(
                    db.queryIndex(db.EDGE_AERO_SET, E_IN_INDEX, Filter.contains(Direction.IN.name(),
                            IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy),
                    db.queryIndex(db.EDGE_AERO_SET, E_OUT_INDEX, Filter.contains(Direction.OUT.name(),
                            IndexCollectionType.DEFAULT, (long) id.getStorageId()), queryPolicy));
        }
        return IteratorUtils.map(iterator, keyRecordEntry ->
                FireflyIdFactory.createId(keyRecordEntry.key.userKey.getObject()));
    }

    @Override
    public long getEdgeCount(final Direction direction) {
        if (direction == Direction.BOTH) {
            LOG.warn("getEdgeCount invoked with direction BOTH - the return value will be correct, but this method " +
                    "is only supposed to be invoked by FireflyVertexLocalCountStep which should never pass in BOTH.");
            return getEdgeCount(Direction.IN) + getEdgeCount(Direction.OUT);
        }

        if (direction == Direction.IN) {
            if (!this.isEdgeCacheDisabled) {
                return this.inEdgeCount;
            } else {
                return IteratorUtils.count(getInEdgeIdsIter());
            }
        } else {
            if (!this.isEdgeCacheDisabled) {
                return this.outEdgeCount;
            } else {
                return IteratorUtils.count(getOutEdgeIdsIter());
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
    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String set, final Expression exp,
                                                                   final ScanPolicy policy) {
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
     * Remove edge from vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    @Override
    protected void removeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        // Edge cache is disabled for this vertex - do nothing.
        if (this.isEdgeCacheDisabled) {
            return;
        }

        // Get bin names for edge direction.
        final String counterBinName = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;

        // Get key for this vertex in database.
        final Key key = getKey(this.db.getNamespace(), this.db.VERTEX_AERO_SET, this.id);

        // Create operations for removing from edge cache.
        final Bin edgeCounter = new Bin(counterBinName, -1L);
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED);
        final Operation decrementEdgeCounter = Operation.add(edgeCounter);
        final Operation getEdgeCounter = Operation.get(counterBinName);
        final Operation removeEdgeId = ListOperation.removeByValue(
                cacheBinName,
                Value.get(edgeId.getCachedId()),
                ListReturnType.NONE,
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );
        final Operation removeEmptyEdgeCacheKeys = MapOperation.removeByValue(cacheBinName,
                Value.get(new ArrayList<>()), MapReturnType.COUNT);

        // Operate on database.
        final FireflyCache cache = db.transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        final Record results = this.db.getClient().operate(null, key, getCacheDisabled, decrementEdgeCounter,
                getEdgeCounter, removeEdgeId, removeEmptyEdgeCacheKeys);

        final boolean isCacheDisabled = results.getBoolean(this.db.EDGE_CACHE_DISABLED);

        if (isCacheDisabled) {
            // Edge cache was disabled by a different concurrent traversal - only need to update the flag of this.
            this.isEdgeCacheDisabled = true;
        } else {
            // Edge cache still enabled - update the JVM cache of this.
            final Map<String, List<FireflyId>> edgeCache = direction == Direction.IN ? this.inEdgeIds : this.outEdgeIds;
            if (!edgeCache.containsKey(edgeLabel)) {
                LOG.error("Could not find edge label {} in vertex {}. Vertex edge cache did not contain edge id {}.",
                        edgeLabel, this.id, edgeId);
                return;
            }

            final List<FireflyId> edgeIdsOfLabel = edgeCache.get(edgeLabel);
            if (!edgeIdsOfLabel.contains(edgeId)) {
                LOG.error("Could not find edge id {} in vertex {}. Vertex edge cache under label {} did not contain edge id {}.",
                        edgeId, this.id, edgeLabel, edgeId);

            }

            // Remove item from vertex property Map.
            edgeIdsOfLabel.remove(edgeId);

            // Remove key if IDs are empty.
            if (edgeIdsOfLabel.isEmpty()) {
                edgeCache.remove(edgeLabel);
            }

            // Update count.
            // TODO GRAPH-278: Figure out the odd behavior behind the results of counterBinName. More information in
            //                 the JIRA ticket. Just decrement the count by 1 for now instead of syncing with source of
            //                 truth from Aerospike since doing so has extremely limited if any impact.
            //final long edgeCount = results.getLong(counterBinName);
            if (direction == Direction.IN) {
                //this.inEdgeCount = edgeCount;
                this.inEdgeCount--;
            } else {
                //this.outEdgeCount = edgeCount;
                this.outEdgeCount--;
            }
        }

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
        // Edge cache is disabled for this vertex - do nothing.
        if (this.isEdgeCacheDisabled) {
            return;
        }

        // Get bin names for edge direction.
        final String counterBinName = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;

        // Get key for this vertex in database.
        final Key key = getKey(this.db.getNamespace(), this.db.VERTEX_AERO_SET, this.id);

        // Create operations for writing to edge cache.
        final Bin edgeCounter = new Bin(counterBinName, 1);
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED);
        final Operation incrementEdgeCounter = Operation.add(edgeCounter);
        final Operation getEdgeCounter = Operation.get(counterBinName);
        final Operation appendToEdgeCache = ListOperation.append(
                cacheBinName,
                Value.get(edgeId.getCachedId()),
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );

        // Operate on database.
        final Record results;
        try {
            final FireflyCache cache = db.transactionCache.get();
            if (cache != null) {
                cache.invalidate(key);
            }
            results = this.db.getClient().operate(null, key, getCacheDisabled, incrementEdgeCounter,
                    getEdgeCounter, appendToEdgeCache);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.RECORD_TOO_BIG)
                throw new RuntimeException(RECORD_TOO_BIG_ERROR);
            throw ae;
        }

        final boolean edgeCacheDisabled = results.getBoolean(this.db.EDGE_CACHE_DISABLED);
        final long edgeCount = results.getLong(counterBinName);

        if (edgeCacheDisabled) {
            // Cache was disabled by a different concurrent traversal - just update the flag for this vertex.
            this.isEdgeCacheDisabled = true;
        } else if (edgeCount > this.db.ID_CACHE_SIZE) {
            // Adding the edge hit the edge cache disable trigger size.

            // Disable the edge cache.
            final Bin disabledEdgeCacheBin = new Bin(this.db.EDGE_CACHE_DISABLED, true);
            final Operation disableEdgeCache = Operation.put(disabledEdgeCacheBin);

            // Wipe both the edge caches.
            final Bin emptyInCache = new Bin(this.db.IN_EDGES, Value.get(new TreeMap<String, List<Object>>()));
            final Bin emptyOutCache = new Bin(this.db.IN_EDGES, Value.get(new TreeMap<String, List<Object>>()));
            final Operation wipeInCache = Operation.put(emptyInCache);
            final Operation wipeOutCache = Operation.put(emptyOutCache);

            final FireflyCache cache = db.transactionCache.get();
            if (cache != null) {
                cache.invalidate(key);
            }
            this.db.getClient().operate(null, key, disableEdgeCache, wipeInCache, wipeOutCache);

            // Update cache disabled flag for this vertex.
            this.isEdgeCacheDisabled = true;
        } else {
            // The cache is still enabled so update it.
            if (direction == Direction.IN) {
                if (!this.inEdgeIds.containsKey(edgeLabel)) {
                    this.inEdgeIds.put(edgeLabel, new ArrayList<>());
                }
                this.inEdgeIds.get(edgeLabel).add(edgeId);
                this.inEdgeCount = edgeCount;
            } else {
                if (!this.outEdgeIds.containsKey(edgeLabel)) {
                    this.outEdgeIds.put(edgeLabel, new ArrayList<>());
                }
                this.outEdgeIds.get(edgeLabel).add(edgeId);
                this.outEdgeCount = edgeCount;
            }
        }
    }

    /**
     * Get property id map and write to vertex property set.
     *
     * @param graph      Graph to use.
     * @param properties Properties.
     * @param vertexId   Vertex id.
     * @return Property id map.
     */
    private static Map<String, List<FireflyId>> getPropertyIdMapAndWrite(final FireflyGraph graph,
                                                                         final List<Map.Entry<String, Object>> properties,
                                                                         final FireflyId vertexId) {
        final AerospikeConnection db = graph.getBaseGraph();

        // Loop through properties and populate the vertex property id cache and vertex property label id map.
        final Map<String, List<FireflyId>> vertexPropertyLabelIdMap = new TreeMap<>();
        final Map<String, List<Object>> vertexPropertyValueMap = new TreeMap<>();
        properties.forEach(vp -> {
            if (!vertexPropertyValueMap.containsKey(vp.getKey())) {
                vertexPropertyValueMap.put(vp.getKey(), new ArrayList<>());
            }
            vertexPropertyValueMap.get(vp.getKey()).add(vp.getValue());
        });
        vertexPropertyValueMap.forEach((key, value) -> value.forEach(v -> {
                    // Get id for vertex property.
                    final FireflyId vertexPropertyId =
                            FireflyIdFactory.createFromManager(graph, FireflyVertexProperty.class);

                    // Add to vertex property ids to map.
                    if (!vertexPropertyLabelIdMap.containsKey(key)) {
                        vertexPropertyLabelIdMap.put(key, new ArrayList<>());
                    }
                    vertexPropertyLabelIdMap.get(key).add(vertexPropertyId);

                    // Create a bin for the vertex property name (key) and a bin for the vertex property id.
                    final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, key);
                    final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, vertexId.getStorageId());

                    // Write vertex property with type hint.
                    db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vertexPropertyId, db.KEY_VALUE, key, v,
                            db.VP_TYPE_HINTS, vpkBin, pviBin);
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
    public static PropertyValueIdMaps getPropertyValueIdMaps(final FireflyGraph graph,
                                                             final List<Map.Entry<String, Object>> properties) {
        // Loop through properties nad populate the vertex properties value map.
        final Map<String, Object> vertexPropertyValueMap = new TreeMap<>();
        final Map<String, FireflyId> vertexPropertyIdMap = new TreeMap<>();
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
        final boolean isEdgeCacheDisabled = db.EDGE_CACHE_DISABLED_GLOBALLY;
        final Map<String, ?> vertexPropertyIds;
        final Map<String, ?> vertexPropertyIdsWritable;
        final Map<String, Object> vertexPropertyValueMap;

        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                vertexPropertyIds = getPropertyIdMapAndWrite(graph, properties, vertexId);
                vertexPropertyIdsWritable =
                        FireflyIdFactory.convertMapListToStorage((Map<String, List<FireflyId>>) vertexPropertyIds);
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

        // Create vertex bins for cache state, vertex label, and property ids.
        final Bin cacheDisabledBin = new Bin(db.EDGE_CACHE_DISABLED, Value.get(isEdgeCacheDisabled));
        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin vertexPropertyIdsBin;
        final Bin typeHint = new Bin(db.RELATIONAL_VERTEX_TYPE_HINT, Value.get(vertexTypeHint));
        final Map<String, List<Long>> uninitalizedEdgeCacheIn = new TreeMap<>();
        final Bin edgeCacheInBin =
                new Bin(Direction.IN.name(), Value.get(uninitalizedEdgeCacheIn, MapOrder.KEY_ORDERED));
        final Map<String, List<Long>> uninitalizedEdgeCacheOut = new TreeMap<>();
        final Bin edgeCacheOutBin =
                new Bin(Direction.OUT.name(), Value.get(uninitalizedEdgeCacheOut, MapOrder.KEY_ORDERED));
        final Map<String, Long> vertexPropertyTypeHintMap;

        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                final Bin vertexPropertyCounterBin = new Bin(db.VP_COUNTER, Value.get(properties.size()));

                final boolean isVertexPropertyCacheDisabled = properties.size() > db.ID_CACHE_SIZE;
                final Bin vertexPropertyCacheDisabledBin =
                        new Bin(db.VP_CACHE_DISABLED, Value.get(isVertexPropertyCacheDisabled));

                // If the VP cache is disabled, don't store VPs.
                vertexPropertyIdsBin = isVertexPropertyCacheDisabled ?
                        new Bin(db.VERTEX_PROPERTY_NAME_TO_ID,
                                Value.get(new TreeMap<>(), MapOrder.KEY_ORDERED)) :
                        new Bin(db.VERTEX_PROPERTY_NAME_TO_ID,
                                Value.get(vertexPropertyIdsWritable, MapOrder.KEY_ORDERED));

                FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, -1, cacheDisabledBin, labelBin,
                        vertexPropertyIdsBin, vertexPropertyCounterBin, vertexPropertyCacheDisabledBin, typeHint);
                return new LinkedVertex(vertexId, label, graph, new TreeMap<>(), new TreeMap<>(), 0, 0,
                        (Map<String, List<FireflyId>>) vertexPropertyIds, isVertexPropertyCacheDisabled,
                        isEdgeCacheDisabled, db);
            case StarPackedVertex.VERTEX_TYPE_HINT:
                // Star specific
                // Fall through
            case PackedVertex.VERTEX_TYPE_HINT:
                vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIdsWritable,
                        MapOrder.KEY_ORDERED));
                final Bin vertexPropertyValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE,
                        Value.get(vertexPropertyValueMap, MapOrder.KEY_ORDERED));
                vertexPropertyTypeHintMap = new TreeMap<>();
                for (Map.Entry<String, ?> entry : vertexPropertyValueMap.entrySet()) {
                    final Long supportedType;
                    if (entry.getValue() == null) {
                        supportedType = null;
                    } else {
                        supportedType = db.getSupportedType(entry.getValue().getClass());
                    }
                    vertexPropertyTypeHintMap.put(entry.getKey(), supportedType);
                }
                final Bin vertexPropertyValuesTypeHintsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT,
                        Value.get(vertexPropertyTypeHintMap, MapOrder.KEY_ORDERED));

                // Set generation to -1 (no generation check) because this is the initial write of the vertex.
                FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, -1,
                        cacheDisabledBin,
                        labelBin,
                        edgeCacheOutBin,
                        edgeCacheInBin,
                        vertexPropertyIdsBin,
                        vertexPropertyValuesBin,
                        vertexPropertyValuesTypeHintsBin,
                        typeHint);
                return PackedVertex.PackedVertexFactory.create(vertexId, label, graph, new TreeMap<>(), new TreeMap<>(),
                        0, 0, (Map<String, FireflyId>) vertexPropertyIds, vertexPropertyValueMap,
                        null, isEdgeCacheDisabled, db);
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

        // Get cache state.
        final boolean edgeCacheDisabled = record.getBoolean(db.EDGE_CACHE_DISABLED);

        // Get count of IN and OUT edges.
        final long inEdgeCount = record.getLong(db.IN_EDGE_COUNTER);
        final long outEdgeCount = record.getLong(db.OUT_EDGE_COUNTER);

        // Get inEdgeIds and outEdgeIds. If the cache is disabled default to an empty map.
        final Map<String, List<Object>> inEdgeIds = edgeCacheDisabled ?
                new TreeMap<>() : (Map<String, List<Object>>) record.getMap(db.IN_EDGES);
        final Map<String, List<Object>> outEdgeIds = edgeCacheDisabled ?
                new TreeMap<>() : (Map<String, List<Object>>) record.getMap(db.OUT_EDGES);
        final Map<String, List<FireflyId>> fireflyInEdgeIds =
                FireflyIdFactory.convertMapListObjectToFireflyIdMap(inEdgeIds);
        final Map<String, List<FireflyId>> fireflyOutEdgeIds =
                FireflyIdFactory.convertMapListObjectToFireflyIdMap(outEdgeIds);
        // Create vertex based on type hint.
        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT: {
                // Get vertex properties from record.
                final boolean isVertexPropertyCacheDisabled = record.getBoolean(db.VP_CACHE_DISABLED);
                final Map<String, List<Object>> vertexProperties = isVertexPropertyCacheDisabled ?
                        new TreeMap<>() : (Map<String, List<Object>>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
                final Map<String, List<FireflyId>> fireflyVertexProperties =
                        FireflyIdFactory.convertMapListObjectToFireflyIdMap(vertexProperties);
                return new LinkedVertex(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds, inEdgeCount,
                        outEdgeCount, fireflyVertexProperties, isVertexPropertyCacheDisabled, edgeCacheDisabled, db);
            }
            case StarPackedVertex.VERTEX_TYPE_HINT:
                // The type hint of StarPackedVertex is currently not used and is stored in DB as Packed - fall through
            case PackedVertex.VERTEX_TYPE_HINT:
                // Get vertex properties and vertex property counter from record.
                final Map<String, Object> vertexPropertyValues =
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
                final Map<String, Long> vertexPropertyTypeHints =
                        (Map<String, Long>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
                vertexPropertyValues.replaceAll((k, v) -> db.convertValuetoTypeUsingHint(vertexPropertyValues.get(k),
                        vertexPropertyTypeHints.get(k)));
                final Map<String, Object> vertexPropertyIds =
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
                final Map<String, FireflyId> fireflyVertexPropertyIds =
                        FireflyIdFactory.convertMapObjectToFireflyIdMap(vertexPropertyIds);
                return PackedVertex.PackedVertexFactory.create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                        inEdgeCount, outEdgeCount, fireflyVertexPropertyIds, vertexPropertyValues,
                        vertexPropertyTypeHints, edgeCacheDisabled, db);
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
    public int appendAdjacentVertexIds(final List<FireflyId> adjacentVertexIds, final Direction direction,
                                       final String... edgeLabels) {
        int i = 0;
        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (this.isEdgeCacheDisabled) {
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
            if (this.isEdgeCacheDisabled) {
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

    /**
     * Return whether caching is disabled for this vertex.
     *
     * @return is the cache disabled.
     */
    @Override
    public boolean isEdgeCacheDisabled() {
        return this.isEdgeCacheDisabled;
    }
}
