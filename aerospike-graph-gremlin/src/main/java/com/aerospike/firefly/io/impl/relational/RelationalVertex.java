package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.Bin;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.ConcurrentScanRecordSequenceListener;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.utils.OperationReturnHandler;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromIndexedVertex;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.utils.OperationReturnHandler.getValueAtIndex;

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
    protected boolean isEdgeCacheOverflowed;
    protected final AerospikeConnection db;

    /**
     * Constructor for RelationalVertex.
     *
     * @param fid                   firefly id.
     * @param label                 label.
     * @param graph                 graph.
     * @param inEdgeIds             incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds            outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount           incoming edge count.
     * @param outEdgeCount          outgoing edge count.
     * @param isEdgeCacheOverflowed is the edge cache overflowed.
     * @param db                    Aerospike connection.
     */
    protected RelationalVertex(final FireflyId fid,
                               final String label,
                               final FireflyGraph graph,
                               final Map<String, List<FireflyId>> inEdgeIds,
                               final Map<String, List<FireflyId>> outEdgeIds,
                               final long inEdgeCount,
                               final long outEdgeCount,
                               final boolean isEdgeCacheOverflowed,
                               final AerospikeConnection db) {
        super(fid, label, graph);
        this.inEdgeIds = inEdgeIds == null ? new TreeMap<>() : inEdgeIds;
        this.outEdgeIds = outEdgeIds == null ? new TreeMap<>() : outEdgeIds;
        this.inEdgeCount = inEdgeCount;
        this.outEdgeCount = outEdgeCount;
        this.isEdgeCacheOverflowed = isEdgeCacheOverflowed;
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
            // If edge id is composite remove composition and get edge id directly.
            if (edgeId instanceof FireflyIdComposite) {
                edgeId = ((FireflyIdComposite) edgeId).getEdgeId();
            }

            // Remove edge via id without materializing the edge into memory.
            graph.removeEdgeById(edgeId);
        });

        removeVertexProperties();

        // Remove vertex.
        LOG.debug("Removing vertex {}.", id);
        db.delete(FireflyRecord.getKey(db, db.VERTEX_AERO_SET, id));

        graph.fireflySummaryUpdater.addVertexRemoveToQueue(label);

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
        final List<FireflyEdge> edges = RelationalEdge.readEdges(this.graph, edgeIds);

        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        final List<Vertex> listOfEdges = edges.stream()
                .filter(edge -> edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(edge.label()))
                .flatMap(edge -> {
                    if (id().equals(edge.outVertex().id()) && id().equals(edge.inVertex().id()))
                        return Stream.of(this);
                    return FireflyCloseableIteratorUtils.stream(FireflyCloseableIteratorUtils.filter(edge.vertices(direction.opposite()),
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
        if (this.isEdgeCacheOverflowed) {
            final List<FireflyId> edgeIds = getEdgeIdsFromVertex(direction);
            vertices.addAll(verticesFromEdgeIds(edgeIds, direction, edgeLabels));
        } else {
            final List<FireflyId> vertexIds = new ArrayList<>();
            appendAdjacentVertexIds(vertexIds, direction, edgeLabels);
            List<FireflyRecord> records = FireflyRecord.batchRead(db, db.VERTEX_AERO_SET, vertexIds);
            if (records != null) {
                records.forEach(record -> {
                    if (record != null) {
                        vertices.add(fromRecord(graph, new KeyRecord(record.key(), record.record())));
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
        if (this.isEdgeCacheOverflowed && !graph.getBaseGraph().ADJACENCY_INDEX_ENABLED_FLAG) {
            return getEdgeIdsFromVertexByScan(Direction.IN); // Fall back to scan if no index and cache is blown
        }
        // Get cached IDs
        final List<FireflyId> cachedIds = new ArrayList<>();
        if (inEdgeIds != null) {
            inEdgeIds.values().forEach(cachedIds::addAll);
        }
        final Iterator<FireflyId> cachedIdsIter = new FireflyCloseableIterator<>(cachedIds.iterator());
        if (this.isEdgeCacheOverflowed) {
            // Get overflow IDs using index
            final Iterator<FireflyId> overflowIds = getEdgeIdsFromVertexByIndex(Direction.IN);
            return FireflyCloseableIteratorUtils.concat(cachedIdsIter, overflowIds);
        } else {
            return cachedIdsIter;
        }
    }

    private List<FireflyId> getInEdgeIds() {
        return FireflyCloseableIteratorUtils.list(getInEdgeIdsIter());
    }

    /**
     * Get outgoing edge ids for vertex.
     *
     * @return Iterator of all outgoing edge ids.
     */
    private Iterator<FireflyId> getOutEdgeIdsIter() {
        if (this.isEdgeCacheOverflowed && !graph.getBaseGraph().ADJACENCY_INDEX_ENABLED_FLAG) {
            return getEdgeIdsFromVertexByScan(Direction.OUT); // Fall back to scan if no index and cache is blown
        }
        // Get cached IDs
        final List<FireflyId> cachedIds = new ArrayList<>();
        if (outEdgeIds != null) {
            outEdgeIds.values().forEach(cachedIds::addAll);
        }
        final Iterator<FireflyId> cachedIdsIter = new FireflyCloseableIterator<>(cachedIds.iterator());
        if (this.isEdgeCacheOverflowed) {
            // Get overflow IDs using index
            final Iterator<FireflyId> overflowIds = getEdgeIdsFromVertexByIndex(Direction.OUT);
            return FireflyCloseableIteratorUtils.concat(cachedIdsIter, overflowIds);
        } else {
            return cachedIdsIter;
        }
    }

    private List<FireflyId> getOutEdgeIds() {
        return FireflyCloseableIteratorUtils.list(getOutEdgeIdsIter());
    }

    /**
     * Get incoming and outgoing edge ids for vertex.
     *
     * @return Iterator of all incoming and outgoing edge ids.
     */
    private List<FireflyId> getBothEdgeIds() {
        return FireflyCloseableIteratorUtils.list(FireflyCloseableIteratorUtils.concat(getOutEdgeIdsIter(), getInEdgeIdsIter()));
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
            final String binName = direction == Direction.OUT ? Direction.OUT.name() : Direction.IN.name();
            exp = Exp.build(
                    Exp.gt(
                            MapExp.getByValue(MapReturnType.COUNT, Exp.val(this.id.getKeyHashBase64()), Exp.mapBin(binName)),
                            Exp.val(0)
                    ));
        } else {
            // If direction is both, we need to get in and out.
            exp = Exp.build(
                    Exp.or(
                            Exp.gt(
                                    MapExp.getByValue(MapReturnType.COUNT, Exp.val(this.id.getKeyHashBase64()), Exp.mapBin(Direction.IN.name())),
                                    Exp.val(0)
                            ),
                            Exp.gt(
                                    MapExp.getByValue(MapReturnType.COUNT, Exp.val(this.id.getKeyHashBase64()), Exp.mapBin(Direction.OUT.name())),
                                    Exp.val(0)
                            )
                    ));
        }
        // Create scan policy, need bin data for this.
        final ScanPolicy policy = new ScanPolicy();
        policy.includeBinData = true;
        final Iterator<KeyRecord> i = scanAllRecordsInSet(db.EDGE_AERO_SET, exp, policy);
        return new FireflyPhatEdgeIdIteratorFromVertex(i, this.db, direction, this.id);

    }

    protected Iterator<FireflyId> getEdgeIdsFromVertexByIndex(final Direction direction) {
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.sendKey = true;
        queryPolicy.includeBinData = true;
        final Iterator<KeyRecord> keyRecordIterator;
        if (direction == Direction.OUT) {
            keyRecordIterator = db.queryIndex(db.EDGE_AERO_SET, db.E_OUT_INDEX_NAME, Filter.contains(db.SUPERNODES_OUT_BIN,
                    IndexCollectionType.MAPVALUES, id.getKeyHashBase64()), queryPolicy);
        } else if (direction == Direction.IN) {
            keyRecordIterator = db.queryIndex(db.EDGE_AERO_SET, db.E_IN_INDEX_NAME, Filter.contains(db.SUPERNODES_IN_BIN,
                    IndexCollectionType.MAPVALUES, id.getKeyHashBase64()), queryPolicy);
        } else {
            keyRecordIterator = FireflyCloseableIteratorUtils.concat(
                    db.queryIndex(db.EDGE_AERO_SET, db.E_OUT_INDEX_NAME, Filter.contains(db.SUPERNODES_OUT_BIN,
                            IndexCollectionType.MAPVALUES, id.getKeyHashBase64()), queryPolicy),
                    db.queryIndex(db.EDGE_AERO_SET, db.E_IN_INDEX_NAME, Filter.contains(db.SUPERNODES_IN_BIN,
                            IndexCollectionType.MAPVALUES, id.getKeyHashBase64()), queryPolicy));
        }

        return new FireflyPhatEdgeIdIteratorFromIndexedVertex(keyRecordIterator, this.db, direction, this.id);
    }

    @Override
    public long getEdgeCount(final Direction direction) {
        if (direction == Direction.BOTH) {
            LOG.warn("getEdgeCount invoked with direction BOTH - the return value will be correct, but this method " +
                    "is only supposed to be invoked by FireflyVertexLocalCountStep which should never pass in BOTH.");
            return getEdgeCount(Direction.IN) + getEdgeCount(Direction.OUT);
        }

        if (direction == Direction.IN) {
            if (!this.isEdgeCacheOverflowed) {
                return this.inEdgeCount;
            } else if (this.db.ADJACENCY_INDEX_ENABLED_FLAG) {
                return this.inEdgeCount + FireflyCloseableIteratorUtils.count(getEdgeIdsFromVertexByIndex(direction));
            } else {
                return FireflyCloseableIteratorUtils.count(getInEdgeIdsIter());
            }
        } else {
            if (!this.isEdgeCacheOverflowed) {
                return this.outEdgeCount;
            } else if (this.db.ADJACENCY_INDEX_ENABLED_FLAG) {
                return this.outEdgeCount + FireflyCloseableIteratorUtils.count(getEdgeIdsFromVertexByIndex(direction));
            } else {
                return FireflyCloseableIteratorUtils.count(getOutEdgeIdsIter());
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
     * @return Iterator of KeyRecord
     */
    protected Iterator<KeyRecord> scanAllRecordsInSet(final String set, final Expression exp, final ScanPolicy policy) {
        LOG.trace("Issuing scan query of all records in {}:{} with filter {}.",
                db.getNamespace(), set, exp);
        final Monitor scanMonitor = new Monitor();
        policy.sendKey = true;
        if (exp != null)
            policy.filterExp = exp;
        final AerospikeClient client = db.getClient();
        final UUID scanId = UUID.randomUUID();
        final ConcurrentScanRecordSequenceListener listener =
                ConcurrentScanRecordSequenceListener.create(db, scanMonitor, scanId);
        listener.setStartTime();
        client.scanAll(db.getEventLoops().next(), listener, policy, db.getNamespace(), set);
        return new FireflyCloseableIterator<>(listener);
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
        // Get bin names for edge direction.
        final String counterBinName = direction == Direction.IN ? db.IN_EDGE_COUNTER_BIN : db.OUT_EDGE_COUNTER_BIN;
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES_BIN : db.OUT_EDGES_BIN;

        // Update the JVM cache of this.
        final Map<String, List<FireflyId>> edgeCache = direction == Direction.IN ? this.inEdgeIds : this.outEdgeIds;

        // If the edge is not in the JVM cache it also means it wasn't read from DB, so no need to operate on DB to
        // remove what isn't there.
        if (!edgeCache.containsKey(edgeLabel)) {
            if (!this.isEdgeCacheOverflowed) {
                LOG.error("Could not find edge label {} in vertex {}. Vertex edge cache did not contain edge id {}.",
                        edgeLabel, this.id, edgeId);
            }
            return;
        }
        final List<FireflyId> edgeIdsOfLabel = edgeCache.get(edgeLabel);
        if (!edgeIdsOfLabel.contains(edgeId)) {
            if (!this.isEdgeCacheOverflowed) {
                LOG.error("Could not find edge id {} in vertex {}. Vertex edge cache under label {} did not contain edge id {}.",
                        edgeId, this.id, edgeLabel, edgeId);
            }
            return;
        }

        // Remove item from vertex property Map.
        edgeIdsOfLabel.remove(edgeId);

        // Remove key if IDs are empty.
        if (edgeIdsOfLabel.isEmpty()) {
            edgeCache.remove(edgeLabel);
        }

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, this.id);

        // Create operation for decrementing the counter.
        final Expression decrementCounterExp = Exp.build(
                Exp.cond(
                        Exp.gt(
                                ListExp.getByValue(
                                        ListReturnType.COUNT,
                                        Exp.val((byte[]) edgeId.getCachedId()),
                                        Exp.mapBin(cacheBinName),
                                        CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)),
                                Exp.val(0)
                        ),
                        Exp.sub(Exp.intBin(counterBinName), Exp.val(1)),
                        Exp.intBin(counterBinName)
                )
        );
        final Operation decrementEdgeCounter = ExpOperation.write(counterBinName, decrementCounterExp, ExpWriteFlags.UPDATE_ONLY);
        // Create operations for removing from edge cache.
        final Operation removeEdgeId = ListOperation.removeByValue(
                cacheBinName,
                Value.get(edgeId.getCachedId()),
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
        final Operation getEdgeCounter = Operation.get(counterBinName);

        // Removing an edge can never change the state of the edge cache so only need to read in case of cache disabling
        // due to concurrent traversals.
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED_BIN);

        // Operate on database.
        final Record results = this.db.operate(null, key, decrementEdgeCounter, removeEdgeId,
                removeEmptyEdgeCacheKeys, getEdgeCounter, getCacheDisabled);

        this.isEdgeCacheOverflowed = results.getBoolean(this.db.EDGE_CACHE_DISABLED_BIN);

        // Update count.
        final long edgeCount = (long) getValueAtIndex(results, counterBinName, 1);
        if (direction == Direction.IN) {
            this.inEdgeCount = edgeCount;
        } else {
            this.outEdgeCount = edgeCount;
        }
    }

    @Override
    public void setCacheDisabled() {
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, this.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.db.operate(writePolicy, key,
                Operation.put(new Bin(this.db.EDGE_CACHE_DISABLED_BIN, Value.get(true))));
        this.isEdgeCacheOverflowed = true;
    }

    /**
     * Write edge to vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     * @return was the edge written to this vertex's edge cache.
     */
    @Override
    public boolean writeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        // Edge cache is overflowed for this vertex - do nothing since writing to overflow bin is on the edge record.
        if (this.isEdgeCacheOverflowed) {
            return false;
        }

        // Get bin names for edge direction.
        final String counterBinName = direction == Direction.IN ? db.IN_EDGE_COUNTER_BIN : db.OUT_EDGE_COUNTER_BIN;
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES_BIN : db.OUT_EDGES_BIN;

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, this.id);

        // TODO GRAPH-667: Remove edge counters as they can't be properly made to be idempotent
        final Bin edgeCounter = new Bin(counterBinName, 1);
        final Operation incrementEdgeCounter = Operation.add(edgeCounter);

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
                                Exp.ge(Exp.intBin(counterBinName), Exp.val(this.db.ON_RECORD_ID_LIMIT))
                        ),
                        Exp.val(true),
                        Exp.val(false)
                )
        );
        final Operation updateCacheState = ExpOperation.write(this.db.EDGE_CACHE_DISABLED_BIN, cacheState, ExpWriteFlags.DEFAULT);
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED_BIN);
        final Operation getEdgeCount = Operation.get(counterBinName);

        // Operate on database.
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        final Record results = this.db.operate(writePolicy, key, incrementEdgeCounter, appendToEdgeCache, updateCacheState,
                getCacheDisabled, getEdgeCount);


        this.isEdgeCacheOverflowed = (boolean) OperationReturnHandler.getValueAtIndex(results, this.db.EDGE_CACHE_DISABLED_BIN, 1);
        final long edgeCount = (long) OperationReturnHandler.getValueAtIndex(results, counterBinName, 1);
        // Update this object's cache in JVM.
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
        return true;
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
            final FireflyId vertexPropertyId = graph.getIdFactory().createFromManager(graph, FireflyVertexProperty.class);

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
     * @param graph                 FireflyGraph to use.
     * @param vertexId              Id of vertex,.
     * @param label                 String label of vertex.
     * @param properties            Map of properties to add to vertex.
     * @param createOnly            Flag that allows only new IDs to be written. Disable only for retry purposes.
     * @param isEdgeCacheOverflowed Initial state of edge cache to set.
     * @return FireflyVertex.
     */
    public static FireflyVertex writeVertex(final FireflyGraph graph,
                                            final FireflyId vertexId,
                                            final String label,
                                            final List<Map.Entry<String, Object>> properties,
                                            final int vertexTypeHint,
                                            final boolean createOnly,
                                            final boolean isEdgeCacheOverflowed) {
        LOG.debug("Writing Vertex {} {}.", vertexId, properties);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();
        final Map<String, ?> vertexPropertyIds;
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
                validProperties.add(property);
            }
        }

        switch (vertexTypeHint) {
            case PackedVertex.VERTEX_TYPE_HINT:
                final PropertyValueIdMaps propertyValueIdMaps = getPropertyValueIdMaps(graph, validProperties);
                vertexPropertyIds = propertyValueIdMaps.idMap;
                vertexPropertyIdsWritable = graph.getIdFactory().convertMapToStorage(propertyValueIdMaps.idMap);
                vertexPropertyValueMap = propertyValueIdMaps.valueMap;
                break;
            default:
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

        switch (vertexTypeHint) {
            case PackedVertex.VERTEX_TYPE_HINT:
                final Bin vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID_BIN,
                        Value.get(vertexPropertyIdsWritable, MapOrder.KEY_ORDERED));
                final Operation writeVertexPropertyIds = Operation.put(vertexPropertyIdsBin);
                final Bin vertexPropertyValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                        Value.get(vertexPropertyValueMap, MapOrder.KEY_ORDERED));
                final Operation writeVertexPropertyValues = Operation.put(vertexPropertyValuesBin);
                final Map<String, Object> vertexPropertyTypeHintMap = new TreeMap<>();
                for (Map.Entry<String, ?> entry : vertexPropertyValueMap.entrySet()) {
                    vertexPropertyTypeHintMap.put(entry.getKey(), AerospikeConnection.getSupportedType(entry.getValue()));
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
                for (final FireflyId id : ((Map<String, FireflyId>) vertexPropertyIds).values()) {
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
                final List<Operation> operations = new ArrayList<>();
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

                db.operate(policy, key, operations.toArray(new Operation[0]));
                graph.fireflySummaryUpdater.addVertexWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
                return PackedVertex.PackedVertexFactory.create(vertexId, label, graph, new TreeMap<>(), new TreeMap<>(),
                        0, 0, (Map<String, FireflyId>) vertexPropertyIds, vertexPropertyValueMap,
                        vertexPropertyTypeHintMap, vpProperties, vpPropertiesTypeHints, isEdgeCacheOverflowed, db);
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }
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
    public static List<FireflyVertex> readVertices(final FireflyGraph graph, final List<HasContainer> hasContainers,
                                                   final List<FireflyId> vertexIds) {
        LOG.debug("Reading vertices {}.", vertexIds);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();

        // Batch read vertex records.
        final List<FireflyRecord> vertexRecords = FireflyRecord.batchRead(
                db,
                graph.hasContainerListToExpression(hasContainers, FireflyVertex.class),
                db.VERTEX_AERO_SET,
                vertexIds);
        if (vertexRecords == null) {
            return new ArrayList<>();
        }

        // Convert records to vertices.
        return vertexRecords.stream().map(record -> fromRecord(graph, new KeyRecord(record.key(), record.record()))).
                collect(Collectors.toList());
    }

    /**
     * Construct vertex from KeyRecord.
     *
     * @param graph     FireflyGraph to use.
     * @param keyRecord KeyRecord to construct vertex with.
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
        final FireflyId id = graph.getIdFactory().createFromRecord(db, FireflyRecord.fromRecord(db, keyRecord),
                FireflyVertex.class);
        final int vertexTypeHint = record.getInt(db.RELATIONAL_VERTEX_TYPE_HINT_BIN);
        final String label = record.getString(db.LABEL_BIN);

        // Get cache state.
        final boolean edgeCacheOverflowed = record.getBoolean(db.EDGE_CACHE_DISABLED_BIN);

        // Get count of IN and OUT edges.
        final long inEdgeCount = record.getLong(db.IN_EDGE_COUNTER_BIN);
        final long outEdgeCount = record.getLong(db.OUT_EDGE_COUNTER_BIN);

        // Get inEdgeIds and outEdgeIds.
        final Map<String, List<Object>> inEdgeIds = new HashMap<>((Map<String, List<Object>>) record.getMap(db.IN_EDGES_BIN));
        final Map<String, List<Object>> outEdgeIds = new HashMap<>((Map<String, List<Object>>) record.getMap(db.OUT_EDGES_BIN));
        final Map<String, List<FireflyId>> fireflyInEdgeIds =
                graph.getIdFactory().convertMapListObjectToFireflyIdMap(inEdgeIds);
        final Map<String, List<FireflyId>> fireflyOutEdgeIds =
                graph.getIdFactory().convertMapListObjectToFireflyIdMap(outEdgeIds);
        final Map<Object, Map<String, Object>> vertexPropertyProperties =
                new HashMap<>((Map<Object, Map<String, Object>>) record.getMap(db.PROPERTIES_BIN));
        final Map<Object, Map<String, Object>> vertexPropertyPropertiesTypeHints =
                new HashMap<>((Map<Object, Map<String, Object>>) record.getMap(db.TYPE_HINTS_BIN));


        // Create vertex based on type hint.
        switch (vertexTypeHint) {
            case PackedVertex.VERTEX_TYPE_HINT:
                // Get vertex properties and vertex property counter from record.
                final Map<String, Object> vertexPropertyValues =
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
                final Map<String, Object> vertexPropertyTypeHints =
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
                final Map<String, Object> vertexPropertyIds =
                        (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID_BIN);
                final Map<String, FireflyId> fireflyVertexPropertyIds =
                        graph.getIdFactory().convertMapObjectToFireflyIdMap(vertexPropertyIds, FireflyVertexProperty.class);
                return PackedVertex.PackedVertexFactory.create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                        inEdgeCount, outEdgeCount, fireflyVertexPropertyIds, vertexPropertyValues,
                        vertexPropertyTypeHints, vertexPropertyProperties, vertexPropertyPropertiesTypeHints,
                        edgeCacheOverflowed, db);
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
    public int appendAdjacentVertexIds(final List<FireflyId> adjacentVertexIds,
                                       final Direction direction,
                                       final String... edgeLabels) {
        int i = 0;
        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (this.isEdgeCacheOverflowed) {
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
            if (this.isEdgeCacheOverflowed) {
                // Should not happen, this is checked before function is called.
                throw new RuntimeException("Error, cannot use appendAdjacentVertexIds unless vertices are cached.");
            }
            for (final Map.Entry<String, List<FireflyId>> entry : this.outEdgeIds.entrySet()) {
                if (edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(entry.getKey())) {
                    for (FireflyId edgeId : entry.getValue()) {
                        FireflyId aid = ((FireflyIdComposite) edgeId).getAdjacentId();
                        adjacentVertexIds.add(aid);
                        i++;
                    }
                }
            }
        }
        return i;
    }

    /**
     * Used by strategies to leverage batch reading edges to get edge ids.
     *
     * @param edgeIds    List of edge ids to append to.
     * @param direction  Direction of edges to get edge ids for.
     * @param edgeLabels Labels of edges to filter with.
     * @return how many vertices were added.
     */
    public int appendEdgeIds(final List<FireflyId> edgeIds,
                             final Direction direction,
                             final String... edgeLabels) {
        int i = 0;
        final Set<String> edgeLabelsSet = Set.of(edgeLabels);
        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (this.isEdgeCacheOverflowed) {
                // Should not happen, this is checked before function is called.
                throw new RuntimeException("Error, cannot use appendAdjacentVertexIds unless vertices are cached.");
            }
            for (final Map.Entry<String, List<FireflyId>> entry : this.inEdgeIds.entrySet()) {
                if (edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(entry.getKey())) {
                    for (final FireflyId edgeId : entry.getValue()) {
                        edgeIds.add(((FireflyIdComposite) edgeId).getEdgeId());
                        i++;
                    }
                }
            }
        }
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            if (this.isEdgeCacheOverflowed) {
                // Should not happen, this is checked before function is called.
                throw new RuntimeException("Error, cannot use appendAdjacentVertexIds unless vertices are cached.");
            }
            for (final Map.Entry<String, List<FireflyId>> entry : this.outEdgeIds.entrySet()) {
                if (edgeLabelsSet.isEmpty() || edgeLabelsSet.contains(entry.getKey())) {
                    for (final FireflyId edgeId : entry.getValue()) {
                        edgeIds.add(((FireflyIdComposite) edgeId).getEdgeId());
                        i++;
                    }
                }
            }
        }
        return i;
    }

    /**
     * Return whether this vertex's edge cache was filled and therefore potentially has edges in the edge set in
     * addition to those currently in the cache.
     *
     * @return is the edge cache overflowed.
     */
    @Override
    public boolean isEdgeCacheOverflowed() {
        return this.isEdgeCacheOverflowed;
    }
}
