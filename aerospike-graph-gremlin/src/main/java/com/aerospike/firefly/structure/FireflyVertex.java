package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
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
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.io.utils.OperationReturnHandler;
import com.aerospike.firefly.io.utils.RecordTooBigException;
import com.aerospike.firefly.io.utils.VertexRecordSizeExceededException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromIndexedVertex;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import com.aerospike.firefly.structure.util.FireflyHelper;
import groovy.util.MapEntry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.utils.OperationReturnHandler.getValueAtIndex;
import static com.aerospike.firefly.io.utils.VertexRecordSizeExceededException.fromAddingToEdgeCache;
import static com.aerospike.firefly.io.utils.VertexRecordSizeExceededException.fromAddingVertexProperty;
import static com.aerospike.firefly.io.utils.VertexRecordSizeExceededException.getRelevantVertexBins;
import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyVertex extends FireflyElement implements Vertex {

    public static final int VERTEX_TYPE_HINT = 1;
    private static final Logger LOG = LoggerFactory.getLogger(RelationalVertex.class);
    protected final Map<String, List<FireflyId>> inEdgeIds;
    protected final Map<String, List<FireflyId>> outEdgeIds;
    protected final AerospikeConnection db;
    protected FireflyGraph graph;
    public static final String SUPERNODE_KEY = "~supernode";
    protected Map<String, FireflyId> vertexPropertyIds;
    protected Map<String, Object> vertexPropertyValues;
    protected Map<String, Object> vertexPropertyValuesTypeHints;
    protected Map<Object, Map<String, Object>> vertexPropertyIdToProperties;
    protected Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints;
    protected long inEdgeCount;
    protected long outEdgeCount;
    protected boolean isEdgeCacheOverflowed;

    public FireflyVertex(final FireflyId fid, final String label, final FireflyGraph graph, final Map<String, List<FireflyId>> inEdgeIds, final Map<String, List<FireflyId>> outEdgeIds, final Map<String, FireflyId> vertexPropertyIds, final Map<String, Object> vertexPropertyValues, final Map<String, Object> vertexPropertyValuesTypeHints, final Map<Object, Map<String, Object>> vertexPropertyIdToProperties, final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints, final long inEdgeCount, final long outEdgeCount, final boolean isEdgeCacheOverflowed, final AerospikeConnection db) {
        super(fid, label);
        this.graph = graph;
        this.inEdgeIds = inEdgeIds == null ? new TreeMap<>() : inEdgeIds;
        this.outEdgeIds = outEdgeIds == null ? new TreeMap<>() : outEdgeIds;
        this.vertexPropertyIds = vertexPropertyIds == null ? new TreeMap<>() : vertexPropertyIds;
        this.vertexPropertyValues = vertexPropertyValues == null ? new TreeMap<>() : vertexPropertyValues;
        this.vertexPropertyValuesTypeHints = vertexPropertyValuesTypeHints == null ? new TreeMap<>() : vertexPropertyValuesTypeHints;
        this.vertexPropertyIdToProperties = vertexPropertyIdToProperties == null ? new TreeMap<>() : vertexPropertyIdToProperties;
        this.vertexPropertyIdToTypeHints = vertexPropertyIdToTypeHints == null ? new TreeMap<>() : vertexPropertyIdToTypeHints;
        this.inEdgeCount = inEdgeCount;
        this.outEdgeCount = outEdgeCount;
        this.isEdgeCacheOverflowed = isEdgeCacheOverflowed;
        this.db = db;
    }

    /**
     * Read vertex properties for the vertex.
     *
     * @return Iterator of String label to List of FireflyVertexProperty
     */
    protected <V> Iterator<Map.Entry<String, VertexProperty<V>>> readVertexProperties() {
        FireflyVertex.LOG.debug("Read vertex properties");

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final List<Map.Entry<String, FireflyVertexProperty<V>>> vertexPropertyList = new ArrayList<>();

        for (final Map.Entry<String, Object> vertexProperty : vertexPropertyValues.entrySet()) {
            final String vpKey = vertexProperty.getKey();
            final Object vpValue = this.db.convertValuetoTypeUsingHint(vertexPropertyValues.get(vpKey),
                    vertexPropertyValuesTypeHints.get(vpKey));
            final FireflyId vpId = graph.getIdFactory().createId(vertexPropertyIds.get(vertexProperty.getKey()), FireflyVertexProperty.class);
            final Map<String, Object> vpProperties = vertexPropertyIdToProperties.containsKey(vpId.getStorageId()) ?
                    vertexPropertyIdToProperties.get(vpId.getStorageId()) : new TreeMap<>();
            final Map<String, Object> vpTypeHints = vertexPropertyIdToTypeHints.containsKey(vpId.getStorageId()) ?
                    vertexPropertyIdToTypeHints.get(vpId.getStorageId()) : new TreeMap<>();

            // Create the property.
            final FireflyId pid = graph.getIdFactory().createId(vertexPropertyIds.get(vertexProperty.getKey()), FireflyVertexProperty.class);
            final FireflyVertexProperty<V> property = new PackedVertexProperty<>(graph, pid, this, vpKey, vpValue, vpProperties, vpTypeHints);
            vertexPropertyList.add(new MapEntry(vertexProperty.getKey(), property));
        }

        return FireflyCloseableIteratorUtils.asIterator(vertexPropertyList);
    }

    /**
     * Get the vertex property by vertex property label for the vertex.
     *
     * @param key vertex property label.
     * @return Iterator of FireflyVertexProperty for provided vertex property label.
     */
    protected <V> Iterator<VertexProperty<V>> readVertexProperty(final String key) {
        FireflyVertex.LOG.debug("Reading vertex property {}", key);

        if (!vertexPropertyValues.containsKey(key)) {
            return Collections.emptyIterator();
        }

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final Object vertexProperty = this.db.convertValuetoTypeUsingHint(vertexPropertyValues.get(key),
                vertexPropertyValuesTypeHints.get(key));
        final FireflyId vertexPropertyId = vertexPropertyIds.get(key);
        final Map<String, Object> vpProperties = vertexPropertyIdToProperties.containsKey(vertexPropertyId.getStorageId()) ?
                vertexPropertyIdToProperties.get(vertexPropertyId.getStorageId()) : new TreeMap<>();
        final Map<String, Object> vpTypeHints = vertexPropertyIdToTypeHints.containsKey(vertexPropertyId.getStorageId()) ?
                vertexPropertyIdToTypeHints.get(vertexPropertyId.getStorageId()) : new TreeMap<>();
        return FireflyCloseableIteratorUtils.of(
                new PackedVertexProperty<>(graph, vertexPropertyId, this, key, vertexProperty, vpProperties, vpTypeHints));
    }

    private void updateVertexPropertyJVMCache(final Map<String, FireflyId> vertexPropertyIds,
                                              final Map<String, Object> vertexPropertyValues,
                                              final Map<String, Object> vertexPropertyValuesTypeHints,
                                              final Map<Object, Map<String, Object>> vertexPropertyIdToProperties,
                                              final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints) {
        this.vertexPropertyIds = vertexPropertyIds;
        this.vertexPropertyValues = vertexPropertyValues;
        this.vertexPropertyValuesTypeHints = vertexPropertyValuesTypeHints;
        this.vertexPropertyIdToProperties = vertexPropertyIdToProperties;
        this.vertexPropertyIdToTypeHints = vertexPropertyIdToTypeHints;
    }

    /**
     * Write vertex property to vertex.
     *
     * @param vertexProperty Vertex property to write to vertex.
     */
    public void writeVertexProperty(final FireflyVertexProperty vertexProperty) {
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, this.id);

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation putValue = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                Value.get(vertexProperty.key()), Value.get(vertexProperty.value()));
        final Operation putTypeHint = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN,
                Value.get(vertexProperty.key()), Value.get(getSupportedType(vertexProperty.value())));
        final Operation putId = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN,
                Value.get(vertexProperty.key()), Value.get(vertexProperty.id.getStorageId()));
        final Operation getValues = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
        final Operation getTypeHints = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
        final Operation getIds = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN);

        // Write key for the vertex property's properties
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation addKeyProperties = MapOperation.put(mapPolicy, this.db.PROPERTIES_BIN,
                Value.get(vertexProperty.id.getStorageId()), Value.get(vertexProperty.properties));
        final Operation addKeyPropertiesTypeHints = MapOperation.put(mapPolicy, this.db.TYPE_HINTS_BIN,
                Value.get(vertexProperty.id.getStorageId()), Value.get(vertexProperty.typeHints));
        final Operation getKeyProperties = Operation.get(this.db.PROPERTIES_BIN);
        final Operation getKeyPropertiesTypeHints = Operation.get(this.db.TYPE_HINTS_BIN);

        final FireflyCache cache = db.transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record result = this.db.operate(writePolicy, key, putValue, putId, putTypeHint, addKeyProperties, addKeyPropertiesTypeHints, getValues, getTypeHints,
                    getIds, getKeyProperties, getKeyPropertiesTypeHints);

            final Map<String, Object> vertexPropertyValues = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN, 1)).orElse(new TreeMap<>());
            final Map<String, Object> vertexPropertyTypeHints = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN, 1)).orElse(new TreeMap<>());
            final Map<String, Object> vertexPropertyIds = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_ID_BIN, 1)).orElse(new TreeMap<>());
            final Map<Object, Map<String, Object>> vertexPropertyIdToProperties = (Map<Object, Map<String, Object>>) Optional.ofNullable(getValueAtIndex(result, this.db.PROPERTIES_BIN, 1)).orElse(new TreeMap<>());
            final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints = (Map<Object, Map<String, Object>>) Optional.ofNullable(getValueAtIndex(result, this.db.TYPE_HINTS_BIN, 1)).orElse(new TreeMap<>());
            final Map<String, FireflyId> vertexPropertyFireflyIds = this.graph.getIdFactory().convertMapObjectToFireflyIdMap(vertexPropertyIds, FireflyVertexProperty.class);

            // Update this PackedVertex in JVM cache
            updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
            graph.fireflySummaryUpdater.addVertexPropertiesWriteToQueue(label, Set.of(vertexProperty.key()));
        } catch (final RecordTooBigException e) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVertexProperty((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, key), this.id, vertexProperty.key());
            FireflyVertex.LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    /**
     * Remove the vertex property from the vertex.
     *
     * @param key              vertex property label.
     * @param vertexPropertyId id of the vertex property to remove.
     */
    public void removeVertexPropertyForModel(final String key, final FireflyId vertexPropertyId) {
        final Key opKey = getKey(this.db, this.db.VERTEX_AERO_SET, this.id);

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
        if (cache != null) {
            cache.invalidate(opKey);
        }
        final Record result = this.db.operate(null, opKey, removeProperty, removePropertyTypeHint,
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
        final Map<String, FireflyId> vertexPropertyFireflyIds =
                this.graph.getIdFactory().convertMapObjectToFireflyIdMap(vertexPropertyIds, FireflyVertexProperty.class);

        // Update this PackedVertex in JVM cache
        updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues,
                vertexPropertyValuesTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
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
                FireflyVertex.LOG.error("Could not find edge label {} in vertex {}. Vertex edge cache did not contain edge id {}.",
                        edgeLabel, this.id, edgeId);
            }
            return;
        }
        final List<FireflyId> edgeIdsOfLabel = edgeCache.get(edgeLabel);
        if (!edgeIdsOfLabel.contains(edgeId)) {
            if (!this.isEdgeCacheOverflowed) {
                FireflyVertex.LOG.error("Could not find edge id {} in vertex {}. Vertex edge cache under label {} did not contain edge id {}.",
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

    /**
     * Write edge to vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     * @return was the edge written to this vertex's edge cache.
     */
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

        // Create operations for writing to edge cache.
        final Bin edgeCounter = new Bin(counterBinName, 1);
        final Operation incrementEdgeCounter = Operation.add(edgeCounter);
        final Operation appendToEdgeCache = ListOperation.append(
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
        try {
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
        } catch (final RecordTooBigException e) {
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingToEdgeCache((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, key), this.id, edgeId);
            FireflyVertex.LOG.error(sizeExceededException.getMessage());
            throw sizeExceededException;
        }
    }

    protected void removeVertexProperties() {
        vertexPropertyIds = new TreeMap<>();
        vertexPropertyValues = new TreeMap<>();
        vertexPropertyValuesTypeHints = new TreeMap<>();
    }

    /**
     * Remove vertex. Any edges attached to adjacent vertices must be removed
     * from the adjacent vertices when the edge is removed.
     */
    @Override
    public void remove() {
        // Collect edges in both directions and remove them all.
        final Set<FireflyId> edgeIds = new HashSet<>(getEdgeIdsFromVertex(Direction.BOTH, Set.of()));
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
     * Get all edge ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public List<FireflyId> getEdgeIdsFromVertex(final Direction direction, final Set<String> labels) {
        FireflyVertex.LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> edgeIds = new ArrayList<>();

        if (!isEdgeCacheOverflowed) {
            edgeIds.addAll(getCachedEdgeIds(direction, labels));
        } else {
            edgeIds.addAll(getSupernodeEdgeIds(direction, labels));

            // IMPORTANT NOTE:
            //  Scan returns duplicates of the local cache so if we scanned (i.e if ADJACENCY_INDEX_ENABLED_FLAG is false),
            //  do not add the local cache to the edgeIds list.
            //  Meanwhile, index only returns the edge ids that are not in the local cache, so no duplicates.
            if (graph.getBaseGraph().ADJACENCY_INDEX_ENABLED_FLAG) {
                edgeIds.addAll(getCachedEdgeIds(direction, labels));
            }
        }
        return edgeIds;
    }

    /**
     * Get all edge ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public List<FireflyId> getVertexIdsFromVertex(final Direction direction, final Set<String> labels) {
        FireflyVertex.LOG.trace("Getting vertex ids from vertex {}.", id);
        final List<FireflyId> vertexIds = new ArrayList<>();

        if (!isEdgeCacheOverflowed) {
            vertexIds.addAll(getCachedVertexIds(direction, labels));
        } else {
            vertexIds.addAll(getSupernodeVertexIds(direction, labels));

            // IMPORTANT NOTE:
            //  Scan returns duplicates of the local cache so if we scanned (i.e if ADJACENCY_INDEX_ENABLED_FLAG is false),
            //  do not add the local cache to the vertexIds list.
            //  Meanwhile, index only returns the vertex ids that are not in the local cache, so no duplicates.
            if (graph.getBaseGraph().ADJACENCY_INDEX_ENABLED_FLAG) {
                vertexIds.addAll(getCachedVertexIds(direction, labels));
            }
        }
        return vertexIds;
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedEdgeIds.
     */
    private List<FireflyId> getSupernodeEdgeIds(final Direction direction, final Set<String> labels) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID);
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getSupernodeVertexIds.
     */
    private List<FireflyId> getSupernodeVertexIds(final Direction direction, final Set<String> labels) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID);
    }

    /**
     * Important note about this function: It only returns ids that are cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getSupernodeVertexIds.
     */
    private List<FireflyId> getCachedEdgeIds(final Direction direction, final Set<String> labels) {
        return getCachedIds(direction, labels).stream().map(id -> {
            if (id instanceof FireflyIdComposite) {
                return ((FireflyIdComposite) id).getEdgeId();
            } else {
                return id;
            }
        }).collect(Collectors.toList());
    }

    /**
     * Important note about this function: It only returns ids that are cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedVertexIds.
     */
    private List<FireflyId> getCachedVertexIds(final Direction direction, final Set<String> labels) {
        return getCachedIds(direction, labels).stream().map(id -> ((FireflyIdComposite) id).getAdjacentId()).collect(Collectors.toList());
    }

    public List<FireflyId> getSupernodeIds(final Direction direction,
                                           final Set<String> labels,
                                           final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType) {
        if (!this.isEdgeCacheOverflowed) {
            return new ArrayList<>();
        }

        LOG.trace("Getting supernode edge ids from vertex {}.", id);
        final List<FireflyId> ids = new ArrayList<>();
        if (!graph.getBaseGraph().ADJACENCY_INDEX_ENABLED_FLAG) {
            // Worst case scenario, we have to scan. At this point we just bite the bullet, system is cheaping out on RAM
            // so performance will suck.
            getIdsFromVertexByScan(direction, labels, outputType).forEachRemaining(ids::add);
        } else {
            final Iterator<FireflyId> idIterator = getIdsFromVertexByIndex(direction, labels, outputType);
            while (idIterator.hasNext()) {
                try {
                    ids.add(idIterator.next());
                } catch (final NoSuchElementException e) {
                    LOG.warn("Error getting supernode ids from vertex {}, this is likely from a concurrent removal.", id, e);
                }
            }
        }
        return ids;
    }

    // Only public for testing, if you use this function outside of testing, you're probably doing something wrong.
    public List<FireflyId> getCachedIds(final Direction direction, final Set<String> labels) {
        LOG.trace("Getting cached adjacent vertex ids from vertex {}.", id);
        // Get cached IDs
        final List<FireflyId> cachedIds = new ArrayList<>();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            for (final String key : outEdgeIds.keySet()) {
                if (!labels.isEmpty() && !labels.contains(key)) {
                    continue;
                }
                cachedIds.addAll(outEdgeIds.get(key));
            }
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            for (final String key : inEdgeIds.keySet()) {
                if (!labels.isEmpty() && !labels.contains(key)) {
                    continue;
                }
                cachedIds.addAll(inEdgeIds.get(key));
            }
        }
        return cachedIds;
    }

    /**
     * Get vertices in a specified direction. This function leverages composite ids when appropriate.
     *
     * @param direction  Direction to get edge ids for.
     * @param edgeLabels Edge labels.
     * @return List of vertices.
     */
    public List<Vertex> getVerticesFromVertex(final Direction direction, final Set<String> edgeLabels) {
        FireflyVertex.LOG.trace("Getting vertices from vertex {}.", id);
        final List<Vertex> vertices = new ArrayList<>();
        final List<FireflyId> adjacentVertices = getVertexIdsFromVertex(direction, edgeLabels);
        final List<FireflyRecord> records = FireflyRecord.batchRead(db, db.VERTEX_AERO_SET, adjacentVertices);
        if (records != null) {
            records.forEach(record -> {
                if (record != null) {
                    vertices.add(FireflyVertex.fromRecord(graph, new KeyRecord(record.key(), record.record())));
                }
            });
        }
        return vertices;
    }

    /**
     * Read vertex property keys.
     *
     * @return Set of vertex property keys.
     */
    protected Set<String> readVertexPropertyKeys() {
        return vertexPropertyValues.keySet();
    }

    public void setCacheDisabled() {
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, this.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.db.operate(writePolicy, key,
                Operation.put(new Bin(this.db.EDGE_CACHE_DISABLED_BIN, Value.get(true))));
        this.isEdgeCacheOverflowed = true;
    }

    /**
     * Function to remove vertex properties.
     *
     * @param key              Vertex property key.
     * @param vertexPropertyId Vertex property id.
     */
    public void removeVertexProperty(final String key, final FireflyId vertexPropertyId) {
        removeVertexPropertyForModel(key, vertexPropertyId);
    }

    /**
     * Create a new vertex property. If the cardinality is {@link VertexProperty.Cardinality#single}, then set the key
     * to the value. If the cardinality is {@link VertexProperty.Cardinality#list}, then add a new value to the key.
     * If the cardinality is {@link VertexProperty.Cardinality#set}, then only add a new value if that value doesn't
     * already exist for the key. If the value already exists for the key, add the provided key value vertex property
     * properties to it.
     *
     * @param cardinality the desired cardinality of the property key
     * @param key         the key of the vertex property
     * @param value       The value of the vertex property
     * @param keyValues   the key/value pairs to turn into vertex property properties
     * @param <V>         the type of the value of the vertex property
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality,
                                          final String key,
                                          final V value,
                                          final Object... keyValues) {
        if (FireflyHelper.inComputerMode(this.graph)) {
            throw new RuntimeException(UNIMPLEMENTED);
        }

        if (this.removed)
            throw elementAlreadyRemoved(Vertex.class, this.id);

        if (SUPERNODE_KEY.equals(key)) {
            setCacheDisabled();
            return VertexProperty.empty();
        }

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);

        // If we do not support null and the value is null, we should return empty.
        if (!allowNullPropertyValues && null == value) {
            final VertexProperty.Cardinality card = null == cardinality ? graph.features().vertex().getCardinality(key) : cardinality;
            if (VertexProperty.Cardinality.single == card)
                properties(key).forEachRemaining(VertexProperty::remove);
            return VertexProperty.empty();
        }

        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) {
            return optionalVertexProperty.get();
        }

        // Verify if this is a supported configuration.
        if (!graph.features().vertex().properties().supportsUserSuppliedIds() &&
                ElementHelper.getIdValue(keyValues).isPresent()) {
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }

        // Create Firefly id for vertex property. If user id is present then we support it based on above code.
        final FireflyId vertexPropertyId = ElementHelper.getIdValue(keyValues).isPresent() ?
                graph.getIdFactory().createId(ElementHelper.getIdValue(keyValues).get(), FireflyVertexProperty.class) :
                graph.getIdFactory().createFromManager(graph, FireflyVertexProperty.class);

        // Write vertex property to graph.

        final VertexProperty<V> vertexProperty = graph.writeVertexProperty(vertexPropertyId, this, key, value, keyValues);

        // Return vertex property.
        return vertexProperty;
    }

    @Override
    public Set<String> keys() {
        return FireflyHelper.inComputerMode((FireflyGraph) graph()) ?
                Vertex.super.keys() : readVertexPropertyKeys();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        FireflyHelper.legalPropertyKeyValueArray(keyValues);

        // Validate edge and vertex.
        if (ElementHelper.getIdValue(keyValues).isPresent() && !graph.features().edge().supportsUserSuppliedIds())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (null == vertex)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (null == label || label.isEmpty())
            throw Graph.Exceptions.argumentCanNotBeNull("label");
        if (isHidden(label))
            throw Edge.Exceptions.labelCanNotBeAHiddenKey(label);
        if (this.removed)
            throw elementAlreadyRemoved(Vertex.class, this.id);

        // Get id for edge.
        final FireflyId edgeId = graph.getIdFactory().createFromManager(graph, FireflyEdge.class);

        // Write fully qualified edge.
        final List<Map.Entry<String, Object>> properties =
                graph.convertFullyQualified(graph.features().edge().supportsNullPropertyValues(), keyValues);
        return graph.writeEdge(edgeId, label, properties, (FireflyVertex) vertex, this);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Iterator<Edge> edgeIterator = FireflyHelper.getEdges(graph, this, direction, edgeLabels);
        return FireflyHelper.inComputerMode(this.graph) ?
                FireflyCloseableIteratorUtils.filter(edgeIterator,
                        edge -> this.graph.graphComputerView.legalEdge(this, edge)) :
                edgeIterator;
    }

    /**
     * Get iterator of edge ids from vertex for specified Direction using a scan.
     * public visibility for testing.
     *
     * @param direction Direction to scan.
     * @return Iterator of edge ids.
     */
    public Iterator<FireflyId> getIdsFromVertexByScan(final Direction direction,
                                                      final Set<String> labels,
                                                      final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType) {
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
        return new FireflyPhatEdgeIdIteratorFromVertex(i, this.db, direction, this.id, labels, outputType);
    }

    protected Iterator<FireflyId> getIdsFromVertexByIndex(final Direction direction,
                                                          final Set<String> labels,
                                                          final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType) {
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
            return FireflyCloseableIteratorUtils.concat(
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(db.queryIndex(db.EDGE_AERO_SET, db.E_OUT_INDEX_NAME, Filter.contains(db.SUPERNODES_OUT_BIN,
                            IndexCollectionType.MAPVALUES, id.getKeyHashBase64()), queryPolicy), this.db, Direction.OUT, this.id, labels, outputType),
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(db.queryIndex(db.EDGE_AERO_SET, db.E_IN_INDEX_NAME, Filter.contains(db.SUPERNODES_IN_BIN,
                            IndexCollectionType.MAPVALUES, id.getKeyHashBase64()), queryPolicy), this.db, Direction.IN, this.id, labels, outputType));
        }
        return new FireflyPhatEdgeIdIteratorFromIndexedVertex(keyRecordIterator, this.db, direction, this.id, labels, outputType);
    }

    public long getEdgeCount(final Direction direction) {
        if (direction == Direction.BOTH) {
            FireflyVertex.LOG.warn("getEdgeCount invoked with direction BOTH - the return value will be correct, but this method " +
                    "is only supposed to be invoked by FireflyVertexLocalCountStep which should never pass in BOTH.");
            return getEdgeCount(Direction.IN) + getEdgeCount(Direction.OUT);
        }

        final long baseCount = direction == Direction.IN ? this.inEdgeCount : this.outEdgeCount;
        return this.isEdgeCacheOverflowed ?
                baseCount + FireflyCloseableIteratorUtils.count(getSupernodeEdgeIds(direction, Set.of())) :
                baseCount;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        final Set<String> edgeLabelSet = new HashSet<>(Arrays.asList(edgeLabels));
        return getVerticesFromVertex(direction, edgeLabelSet).iterator();
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (propertyKeys.length == 1) {
            if (propertyKeys[0] == null)
                return Collections.emptyIterator();
            return readVertexProperty(propertyKeys[0]);
        }


        // Read multiple vertex properties.
        final Iterator<Map.Entry<String, VertexProperty<V>>> vertexProperties = readVertexProperties();

        // Return an iterator over the map.
        return (!vertexProperties.hasNext()) ? Collections.emptyIterator() :
                FireflyCloseableIteratorUtils.map(FireflyCloseableIteratorUtils.filter(vertexProperties,
                                e -> ElementHelper.keyExists(e.getKey(), propertyKeys)),
                        Map.Entry::getValue);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
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

        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {
            final PropertyValueIdMaps propertyValueIdMaps = FireflyVertex.getPropertyValueIdMaps(graph, validProperties);
            vertexPropertyIds = propertyValueIdMaps.idMap;
            vertexPropertyIdsWritable = graph.getIdFactory().convertMapToStorage(propertyValueIdMaps.idMap);
            vertexPropertyValueMap = propertyValueIdMaps.valueMap;
        } else {
            // Should never happen.
            throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }

        // Create vertex bins for cache state, vertex label, and property ids.
        final Bin cacheDisabledBin = new Bin(db.EDGE_CACHE_DISABLED_BIN, Value.get(isEdgeCacheOverflowed));
        final Bin labelBin = new Bin(db.LABEL_BIN, Value.get(label));
        final Bin vertexPropertyIdsBin;
        final Bin typeHint = new Bin(db.RELATIONAL_VERTEX_TYPE_HINT_BIN, Value.get(vertexTypeHint));
        final Map<String, List<Long>> emptyEdgeCache = new TreeMap<>();
        final Bin edgeCacheInBin = new Bin(db.IN_EDGES_BIN, Value.get(emptyEdgeCache, MapOrder.KEY_ORDERED));
        final Bin edgeCacheOutBin = new Bin(db.OUT_EDGES_BIN, Value.get(emptyEdgeCache, MapOrder.KEY_ORDERED));
        final Map<String, Object> vertexPropertyTypeHintMap;

        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {
            vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID_BIN, Value.get(vertexPropertyIdsWritable,
                    MapOrder.KEY_ORDERED));
            final Bin vertexPropertyValuesBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                    Value.get(vertexPropertyValueMap, MapOrder.KEY_ORDERED));
            vertexPropertyTypeHintMap = new TreeMap<>();
            for (Map.Entry<String, ?> entry : vertexPropertyValueMap.entrySet()) {
                vertexPropertyTypeHintMap.put(entry.getKey(), AerospikeConnection.getSupportedType(entry.getValue()));
            }
            final Bin vertexPropertyValuesTypeHintsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN,
                    Value.get(vertexPropertyTypeHintMap, MapOrder.KEY_ORDERED));

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
            final Bin vpPropertiesTypeHintsBin = new Bin(db.TYPE_HINTS_BIN,
                    Value.get(vpPropertiesTypeHints, MapOrder.KEY_ORDERED));

            // Set generation to -1 (no generation check) because this is the initial write of the vertex.
            // Also set writeOnly=true, if vertex already exists we will fail.
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, -1, createOnly,
                    cacheDisabledBin,
                    labelBin,
                    edgeCacheOutBin,
                    edgeCacheInBin,
                    vertexPropertyIdsBin,
                    vertexPropertyValuesBin,
                    vertexPropertyValuesTypeHintsBin,
                    typeHint,
                    vpPropertiesBin,
                    vpPropertiesTypeHintsBin);
            graph.fireflySummaryUpdater.addVertexWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
            return RelationalVertex.PackedVertexFactory.create(vertexId, label, graph, new TreeMap<>(), new TreeMap<>(),
                    0, 0, (Map<String, FireflyId>) vertexPropertyIds, vertexPropertyValueMap,
                    vertexPropertyTypeHintMap, vpProperties, vpPropertiesTypeHints, isEdgeCacheOverflowed, db);
        } else {
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
        return vertexRecords.stream().map(record -> FireflyVertex.fromRecord(graph, new KeyRecord(record.key(), record.record()))).
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
        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {// Get vertex properties and vertex property counter from record.
            final Map<String, Object> vertexPropertyValues =
                    (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
            final Map<String, Object> vertexPropertyTypeHints =
                    (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
            final Map<String, Object> vertexPropertyIds =
                    (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID_BIN);
            final Map<String, FireflyId> fireflyVertexPropertyIds =
                    graph.getIdFactory().convertMapObjectToFireflyIdMap(vertexPropertyIds, FireflyVertexProperty.class);
            return RelationalVertex.PackedVertexFactory.create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                    inEdgeCount, outEdgeCount, fireflyVertexPropertyIds, vertexPropertyValues,
                    vertexPropertyTypeHints, vertexPropertyProperties, vertexPropertyPropertiesTypeHints,
                    edgeCacheOverflowed, db);
        } else {
            // Should never happen.
            throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }
    }

    /**
     * Return whether this vertex's edge cache was filled and therefore potentially has edges in the edge set in
     * addition to those currently in the cache.
     *
     * @return is the edge cache overflowed.
     */
    public boolean isEdgeCacheOverflowed() {
        return this.isEdgeCacheOverflowed;
    }

    static class PropertyValueIdMaps {
        public final Map<String, Object> valueMap;
        public final Map<String, FireflyId> idMap;

        public PropertyValueIdMaps(final Map<String, Object> valueMap, final Map<String, FireflyId> idMap) {
            this.valueMap = valueMap;
            this.idMap = idMap;
        }
    }
}
