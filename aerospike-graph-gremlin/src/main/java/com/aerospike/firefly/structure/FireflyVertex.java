package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
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
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.OperationReturnHandler;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.runtime.exceptions.ElementNotFoundException;
import com.aerospike.firefly.runtime.exceptions.RecordTooBigException;
import com.aerospike.firefly.runtime.exceptions.TtlNotEnabledException;
import com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.id.LazyIdTransform;
import com.aerospike.firefly.structure.iterator.FireflyBatchEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.iterator.FireflyFilteredBatchEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromIndexedVertex;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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
import static com.aerospike.firefly.io.aerospike.OperationReturnHandler.getValueAtIndex;
import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.getUserIdString;
import static com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException.fromAddingToEdgeCache;
import static com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException.fromAddingVertexProperty;
import static com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException.getRelevantVertexBins;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyVertex extends FireflyElement implements Vertex {
    public static final int VERTEX_TYPE_HINT = 1;
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertex.class);
    protected final Map<String, List<LazyIdTransform>> inEdgeIds;
    protected final Map<String, List<LazyIdTransform>> outEdgeIds;
    protected final AerospikeConnection db;
    protected FireflyGraph graph;
    public static final String SUPERNODE_PROPERTY_KEY = "~supernode";
    protected Map<String, LazyIdTransform> vertexPropertyIds;
    protected Map<String, Object> vertexPropertyValues;
    protected Map<String, Object> vertexPropertyValuesTypeHints;
    protected Map<Object, Map<String, Object>> vertexPropertyIdToProperties;
    protected Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints;
    protected boolean isEdgeCacheOverflowed;

    public FireflyVertex(final FireflyId fid,
                         final String label,
                         final FireflyGraph graph,
                         final Map<String, List<LazyIdTransform>> inEdgeIds,
                         final Map<String, List<LazyIdTransform>> outEdgeIds,
                         final Map<String, LazyIdTransform> vertexPropertyIds,
                         final Map<String, Object> vertexPropertyValues,
                         final Map<String, Object> vertexPropertyValuesTypeHints,
                         final Map<Object, Map<String, Object>> vertexPropertyIdToProperties,
                         final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints,
                         final boolean isEdgeCacheOverflowed,
                         final AerospikeConnection db) {
        super(fid, label);
        this.graph = graph;
        this.inEdgeIds = inEdgeIds == null ? new TreeMap<>() : inEdgeIds;
        this.outEdgeIds = outEdgeIds == null ? new TreeMap<>() : outEdgeIds;
        this.vertexPropertyIds = vertexPropertyIds == null ? new TreeMap<>() : vertexPropertyIds;
        this.vertexPropertyValues = vertexPropertyValues == null ? new TreeMap<>() : vertexPropertyValues;
        this.vertexPropertyValuesTypeHints = vertexPropertyValuesTypeHints == null ? new TreeMap<>() : vertexPropertyValuesTypeHints;
        this.vertexPropertyIdToProperties = vertexPropertyIdToProperties == null ? new TreeMap<>() : vertexPropertyIdToProperties;
        this.vertexPropertyIdToTypeHints = vertexPropertyIdToTypeHints == null ? new TreeMap<>() : vertexPropertyIdToTypeHints;
        this.isEdgeCacheOverflowed = isEdgeCacheOverflowed;
        this.db = db;
    }

    /**
     * Read vertex properties for the vertex.
     *
     * @param includeSupernodeVirtualProperty Whether to include the ~supernode virtual property
     * @return Iterator of String label to List of FireflyVertexProperty
     */
    protected <V> Iterator<Map.Entry<String, VertexProperty<V>>> readVertexProperties(
            final boolean includeSupernodeVirtualProperty) {
        LOG.debug("Read vertex properties");

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final List<Map.Entry<String, VertexProperty<V>>> vertexPropertyList = new ArrayList<>();
        if (includeSupernodeVirtualProperty && this.isEdgeCacheOverflowed) {
            vertexPropertyList.add(new AbstractMap.SimpleEntry<>(SUPERNODE_PROPERTY_KEY, new FireflyVirtualSupernodeVertexProperty<>(this)));
        }

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
            final FireflyVertexProperty<V> property = new FireflyVertexProperty<>(graph, pid, this, vpKey, (V) vpValue, vpProperties, vpTypeHints);
            vertexPropertyList.add(new AbstractMap.SimpleEntry<>(vertexProperty.getKey(), property));
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
        LOG.debug("Reading vertex property {}", key);

        if (SUPERNODE_PROPERTY_KEY.equals(key)) {
            if (this.isEdgeCacheOverflowed) {
                return FireflyCloseableIteratorUtils.of(new FireflyVirtualSupernodeVertexProperty<>(this));
            } else {
                return Collections.emptyIterator();
            }
        }

        if (!vertexPropertyValues.containsKey(key)) {
            return Collections.emptyIterator();
        }

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final Object vertexProperty = this.db.convertValuetoTypeUsingHint(vertexPropertyValues.get(key),
                vertexPropertyValuesTypeHints.get(key));
        final FireflyId vertexPropertyId = vertexPropertyIds.get(key).transform();
        final Map<String, Object> vpProperties = vertexPropertyIdToProperties.containsKey(vertexPropertyId.getStorageId()) ?
                vertexPropertyIdToProperties.get(vertexPropertyId.getStorageId()) : new TreeMap<>();
        final Map<String, Object> vpTypeHints = vertexPropertyIdToTypeHints.containsKey(vertexPropertyId.getStorageId()) ?
                vertexPropertyIdToTypeHints.get(vertexPropertyId.getStorageId()) : new TreeMap<>();
        return FireflyCloseableIteratorUtils.of(
                (VertexProperty<V>) new FireflyVertexProperty<>(graph, vertexPropertyId, this, key, vertexProperty, vpProperties, vpTypeHints));
    }

    private void updateVertexPropertyJVMCache(final Map<String, LazyIdTransform> vertexPropertyIds,
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
            this.graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, FireflyVertexProperty.class);
            final Map<String, LazyIdTransform> vertexPropertyFireflyIds = (Map) vertexPropertyIds;

            // Update this FireflyVertex in JVM cache
            updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
            graph.fireflySummaryUpdater.addVertexPropertiesWriteToQueue(label, Set.of(vertexProperty.key()));
        } catch (final RecordTooBigException e) {
            System.out.println("Loading VertexRecordSizeExceededException 1");
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingVertexProperty((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, key), this.id, vertexProperty.key());
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
        this.graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, FireflyVertexProperty.class);;
        final Map<String, LazyIdTransform> vertexPropertyFireflyIds = (Map) vertexPropertyIds;


        // Update this FireflyVertex in JVM cache
        updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues,
                vertexPropertyValuesTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
    }

    /**
     * Remove edge from vertex.
     *
     * @param direction         Direction of edge.
     * @param edgeId            Id of edge.
     * @param edgeLabel         Label of edge.
     */
    protected void removeEdge(final Direction direction, final FireflyPhatEdgeId edgeId, final String edgeLabel) {
        // Get bin name for edge direction.
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES_BIN : db.OUT_EDGES_BIN;

        // Update the JVM cache of this.
        final Map<String, List<LazyIdTransform>> edgeCache = direction == Direction.IN ? this.inEdgeIds : this.outEdgeIds;

        // If the edge is not in the JVM cache it also means it wasn't read from DB, so no need to operate on DB to
        // remove what isn't there.
        if (!edgeCache.containsKey(edgeLabel)) {
            if (!this.isEdgeCacheOverflowed) {
                LOG.error("Could not find edge label {} in vertex {}. Vertex edge cache did not contain edge id {}.",
                        edgeLabel, this.id, edgeId);
            }
            return;
        }
        final List<FireflyId> edgeIdsOfLabel = edgeCache.get(edgeLabel).stream().map(LazyIdTransform::transform).collect(Collectors.toList());
        final int indexOfEdgeToRemove = edgeIdsOfLabel.indexOf(edgeId);
        if (indexOfEdgeToRemove == -1) {
            if (!this.isEdgeCacheOverflowed) {
                LOG.error("Could not find edge id {} in vertex {}. Vertex edge cache under label {} did not contain edge id {}.",
                        edgeId, this.id, edgeLabel, edgeId);
            }
            return;
        }

        // Remove item from vertex property Map.
        final FireflyId compositeIdToRemove = edgeIdsOfLabel.remove(indexOfEdgeToRemove);

        // Remove key if IDs are empty.
        if (edgeIdsOfLabel.isEmpty()) {
            edgeCache.remove(edgeLabel);
        }

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, this.id);

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
            final Record results =
                    this.db.writeOperate(null, key, removeEdgeId, removeEmptyEdgeCacheKeys, getCacheDisabled);

            this.isEdgeCacheOverflowed = results.getBoolean(this.db.EDGE_CACHE_DISABLED_BIN);
        } catch (final ElementNotFoundException enfe) {
            // This Vertex's record was deleted concurrently and thus the record does not exist.
            LOG.debug("Error removing edge id {} from edge cache of vertex {}; the vertex was deleted.",
                    getUserIdString(edgeId.getUserId()), this.id.getUserId());
        } catch (final AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when concurrent traversals remove the same Edge ID from the ECACHE and the 
                // Edge is the last of its Label category, meaning the later traversal will fail due to an operation
                // working under the assumption that the Label exists.
                LOG.warn("Error removing edge id {} from edge cache of vertex {}; this is likely from a concurrent removal.",
                        getUserIdString(edgeId.getUserId()), this.id.getUserId());
            } else {
                throw ae;
            }
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
        LOG.debug("Writing Edge {} to ECACHE of Vertex {} with Direction {}.", edgeId, this, direction);
        // Edge cache is overflowed for this vertex - do nothing since writing to overflow bin is on the edge record.
        if (this.isEdgeCacheOverflowed) {
            return false;
        }

        // Update this object's cache in JVM.
        if (direction == Direction.IN) {
            if (!this.inEdgeIds.containsKey(edgeLabel)) {
                this.inEdgeIds.put(edgeLabel, new ArrayList<>());
            }
            this.inEdgeIds.get(edgeLabel).add(new LazyIdTransform(edgeId, graph));
        } else {
            if (!this.outEdgeIds.containsKey(edgeLabel)) {
                this.outEdgeIds.put(edgeLabel, new ArrayList<>());
            }
            this.outEdgeIds.get(edgeLabel).add(new LazyIdTransform(edgeId, graph));
        }

        // Get bin name for edge direction.
        final String cacheBinName = direction == Direction.IN ? db.IN_EDGES_BIN : db.OUT_EDGES_BIN;

        // Get key for this vertex in database.
        final Key key = getKey(db, this.db.VERTEX_AERO_SET, this.id);

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
                                Exp.ge(Exp.val(getEdgeCount(direction)), Exp.val(this.db.ON_RECORD_ID_LIMIT))
                        ),
                        Exp.val(true),
                        Exp.val(false)
                )
        );
        final Operation updateCacheState = ExpOperation.write(this.db.EDGE_CACHE_DISABLED_BIN, cacheState, ExpWriteFlags.DEFAULT);
        final Operation getCacheDisabled = Operation.get(this.db.EDGE_CACHE_DISABLED_BIN);

        // Operate on database.
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            final Record results = this.db.writeOperate(writePolicy, key, appendToEdgeCache, updateCacheState, getCacheDisabled);

            this.isEdgeCacheOverflowed = (boolean) OperationReturnHandler.getValueAtIndex(results, this.db.EDGE_CACHE_DISABLED_BIN, 1);
            return true;
        } catch (final RecordTooBigException e) {
            System.out.println("Loading VertexRecordSizeExceededException 2");
            final VertexRecordSizeExceededException sizeExceededException =
                    fromAddingToEdgeCache((AerospikeException) e.getCause(), this.db,
                            getRelevantVertexBins(this.db, key), this.id, edgeId);
            LOG.error(sizeExceededException.getMessage());
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
        final Iterator<FireflyEdge> inEdges = new FireflyBatchEdgeIterator(graph, getEdgeIdsFromVertex(Direction.IN, Collections.emptySet(), Collections.emptyList()));
        final Iterator<FireflyEdge> outEdges = new FireflyBatchEdgeIterator(graph, getEdgeIdsFromVertex(Direction.OUT, Collections.emptySet(), Collections.emptyList()));
        // Remove the edges themselves and from the adjacent vertices' edge caches.
        inEdges.forEachRemaining(FireflyEdge::removeSelfAndFromOut);
        outEdges.forEachRemaining(FireflyEdge::removeSelfAndFromIn);

        removeVertexProperties();

        // Remove vertex.
        LOG.debug("Removing vertex {}.", id);
        if (db.delete(FireflyRecord.getKey(db, db.VERTEX_AERO_SET, id))) {
            graph.fireflySummaryUpdater.addVertexRemoveToQueue(label);
        }

        // Set flags to indicate vertex has been removed.
        this.removed = true;
        if (graph.getBaseGraph().IS_AUDIT_LOG_ENABLED) {
            LOG.info("[{}] Dropped vertex with id: {}", graph.getUser(), id());
        }
    }

    public Iterator<FireflyId> getEdgeIdsFromVertex(final Direction direction, final Set<String> labels, List<HasContainer> hasContainers) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> cachedIds = getCachedEdgeIds(direction, labels);

        if (isEdgeCacheOverflowed) {
            return FireflyCloseableIteratorUtils.concat(cachedIds.iterator(), getSupernodeEdgeIds(direction, labels, hasContainers));
        } else {
            return cachedIds.iterator();
        }
    }

    /**
     * Get all edge ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public List<FireflyId> getBatchedEdgeIdsFromVertex(final Direction direction, final Set<String> labels, final List<FireflyId> edgeIdContainer,
                                                       final List<HasContainer> hasContainers) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> edgeIds;
        if (edgeIdContainer == null) {
            edgeIds = new ArrayList<>();
        } else {
            edgeIds = edgeIdContainer;
        }

        edgeIds.addAll(getCachedEdgeIds(direction, labels));
        if (isEdgeCacheOverflowed) {
            final Iterator<FireflyId> superNodeEdgeIds = getSupernodeEdgeIds(direction, labels, hasContainers);
            superNodeEdgeIds.forEachRemaining(edgeIds::add);
        }

        return edgeIds;
    }

    /**
     * Used by FireflyMergeEdgeStep. Leverage adjacency sindex filtering to find any edges between this and a given
     * vertex id.
     *
     * @param direction Direction to get edges for.
     * @param adjacent Id of adjacent vertex to find edges between.
     * @param label Label that edges must match.
     * @param propertyEqFilters Property key-value pairs that edges must match.
     * @return Iterator of edges between this vertex and adjacent that match all filters.
     */
    public CloseableIterator<Edge> getEdgesAdjacentToVertex(final Direction direction, final FireflyId adjacent,
                                                            final String label,
                                                            final Map<String, Object> propertyEqFilters) {
        LOG.debug("Getting Edges from Vertex {} with direction {} and adjacent Vertex {}", this.id, direction, adjacent);
        final Set<String> labels = label == null ? Collections.emptySet() : Collections.singleton(label);
        final List<FireflyId> edgeIds = new ArrayList<>();
        final List<FireflyId> cachedIds = getCachedIds(direction, labels);
        for (final FireflyId id : cachedIds) {
            final FireflyIdComposite composite = (FireflyIdComposite) id;
            if (adjacent.equals(composite.getAdjacentId())) {
                edgeIds.add(composite.getEdgeId());
            }
        }

        final List<HasContainer> fireflyHasContainers = new ArrayList<>();
        final List<HasContainer> aerospikeHasContainers = new ArrayList<>();
        for (final Map.Entry<String, Object> property : propertyEqFilters.entrySet()) {
            final Class valueClass = property.getValue().getClass();
            final HasContainer hasContainer;
            if (P.class.isAssignableFrom(valueClass)) {
                // TODO: This is dumb but ensures that we replicate default Tinkerpop behavior. MergeEdge is functional
                //       when a Predicate is passed in and a match is found, but breaks when a match isn't found because
                //       it tries to write a Predicate as a value on creation of the new Edge. Fix this when, if ever,
                //       Tinkerpop fixes it so that our behavior matches.
                hasContainer = new HasContainer(property.getKey(), (P<?>) property.getValue());
            } else {
                hasContainer = new HasContainer(property.getKey(), P.eq(property.getValue()));
            }
            fireflyHasContainers.add(hasContainer);
            if (Long.class.isAssignableFrom(valueClass) || Integer.class.isAssignableFrom(valueClass) ||
                    String.class.isAssignableFrom(valueClass)) {
                aerospikeHasContainers.add(hasContainer);
            }
        }
        final Iterator<FireflyId> adjacentEdgeIds;
        if (isEdgeCacheOverflowed) {
            final Iterator<FireflyId> sindexEdgeIds = new FireflyPhatEdgeIdIteratorFromIndexedVertex(
                    getEdgeKeyRecordsByIndex(direction, labels,
                            FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, aerospikeHasContainers, adjacent),
                    this.db, direction, this.id, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, adjacent);
            adjacentEdgeIds = FireflyCloseableIteratorUtils.concat(edgeIds.iterator(), sindexEdgeIds);
        } else {
            adjacentEdgeIds = edgeIds.iterator();
        }
        return new FireflyFilteredBatchEdgeIterator<>(graph, adjacentEdgeIds, fireflyHasContainers);
    }

    /**
     * Get all edge ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public Iterator<FireflyId> getVertexIdsFromVertex(final Direction direction, final Set<String> labels) {
        LOG.trace("Getting vertex ids from vertex {}.", id);
        final List<FireflyId> cachedIds = getCachedVertexIds(direction, labels);

        if (isEdgeCacheOverflowed) {
            return FireflyCloseableIteratorUtils.concat(cachedIds.iterator(), getSupernodeVertexIds(direction, labels));
        } else {
            return cachedIds.iterator();
        }
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedEdgeIds.
     */
    private Iterator<FireflyId> getSupernodeEdgeIds(final Direction direction, final Set<String> labels,
                                                final List<HasContainer> hasContainers) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, hasContainers);
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getSupernodeVertexIds.
     */
    public Iterator<FireflyId> getSupernodeVertexIds(final Direction direction, final Set<String> labels) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID, Collections.emptyList());
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

    public Iterator<FireflyId> getSupernodeIds(final Direction direction,
                                           final Set<String> labels,
                                           final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                           final List<HasContainer> hasContainers) {
        if (!this.isEdgeCacheOverflowed) {
            return Collections.emptyIterator();
        }

        LOG.trace("Getting supernode edge ids from vertex {}.", id);
        return getIdsFromVertexByIndex(direction, labels, outputType, hasContainers);
    }

    public List<FireflyId> getCachedIds(final Direction direction, final Set<String> labels) {
        LOG.trace("Getting cached adjacent vertex ids from vertex {}.", id);
        // Get cached IDs
        final List<FireflyId> cachedIds = new ArrayList<>();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            for (final String key : outEdgeIds.keySet()) {
                if (labels.isEmpty() || labels.contains(key)) {
                    cachedIds.addAll(outEdgeIds.get(key).stream().map(LazyIdTransform::transform).collect(Collectors.toList()));
                }
            }
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            for (final String key : inEdgeIds.keySet()) {
                if (labels.isEmpty() || labels.contains(key)) {
                    cachedIds.addAll(inEdgeIds.get(key).stream().map(LazyIdTransform::transform).collect(Collectors.toList()));
                }
            }
        }
        return cachedIds;
    }

    /**
     * Get vertices in a specified direction. This function leverages composite ids when appropriate.
     *
     * @param direction  Direction to get edge ids for.
     * @param edgeLabels Edge labels.
     * @return Iterator of vertices.
     */
    public Iterator<Vertex> getVerticesFromVertex(final Direction direction, final Set<String> edgeLabels) {
        LOG.trace("Getting vertices from vertex {}.", id);
        final Iterator<FireflyId> adjacentVertices = getVertexIdsFromVertex(direction, edgeLabels);
        return new FireflyBatchElementIterator<>(this.graph, adjacentVertices, Collections.emptyList(), this.graph::readVertices, null);
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
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, this.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.db.writeOperate(writePolicy, key,
                Operation.put(new Bin(this.db.EDGE_CACHE_DISABLED_BIN, Value.get(true))));
        this.isEdgeCacheOverflowed = true;
    }

    private void setTtl(final long durationSeconds) {
        final long expirationTime = System.currentTimeMillis() + (durationSeconds * 1000);
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, this.id);
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        final Bin ttlBin = new Bin(this.db.TTL_BIN, expirationTime);
        final Operation writeTtl = Operation.put(ttlBin);
        this.db.writeOperate(writePolicy, key, writeTtl);
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

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed )
            return VertexProperty.empty();
        if (FireflyHelper.inComputerMode(this.graph)) {
            final List<VertexProperty> list = (List) this.graph.graphComputerView.getProperty(this, key);
            if (list.size() == 0)
                return VertexProperty.<V>empty();
            else if (list.size() == 1)
                return list.get(0);
            else
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        } else {
            if(super.property(key) instanceof EmptyProperty)
                return VertexProperty.empty();
            return (VertexProperty<V>) super.property(key);
        }
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
            final VertexProperty<V> vertexProperty = (VertexProperty<V>) this.graph.graphComputerView.addProperty(this, key, value);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            return vertexProperty;
        }

        if (this.removed)
            throw elementAlreadyRemoved(Vertex.class, this.id);

        if (SUPERNODE_PROPERTY_KEY.equals(key)) {
            setCacheDisabled();
            return VertexProperty.empty();
        }

        // Handle TTL.
        if (TTL_PROPERTY_KEY.equals(key)) {
            if (!db.TTL_ENABLED_FLAG) {
                throw new TtlNotEnabledException();
            }
            if (Number.class.isAssignableFrom(value.getClass())) {
                setTtl(((Number) value).longValue());
                return VertexProperty.empty();
            } else {
                throw new IllegalArgumentException(
                        String.format("Property value [%s] for key %s is of type %s and must be numeric", value, key,
                                value.getClass()));
            }
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

    protected Iterator<FireflyId> getIdsFromVertexByIndex(final Direction direction,
                                                          final Set<String> labels,
                                                          final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                          final List<HasContainer> hasContainers) {
        if (direction == Direction.BOTH) {
            final Iterator<KeyRecord> inKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.IN, labels, outputType, hasContainers);
            final Iterator<KeyRecord> outKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.OUT, labels, outputType, hasContainers);
            return FireflyCloseableIteratorUtils.concat(new FireflyPhatEdgeIdIteratorFromIndexedVertex(inKeyRecordIterator, this.db, Direction.IN, this.id, labels, outputType, null),
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(outKeyRecordIterator, this.db, Direction.OUT, this.id, labels, outputType, null));
        } else {
            final Iterator<KeyRecord> keyRecordIterator = getEdgeKeyRecordsByIndex(direction, labels, outputType, hasContainers);
            return new FireflyPhatEdgeIdIteratorFromIndexedVertex(keyRecordIterator, this.db, direction, this.id, labels, outputType, null);
        }
    }

    /**
     * Get the KeyRecord iterator for Edges attached to this Vertex. Public only for testing purposes.
     *
     * @param direction
     * @param labels
     * @param outputType
     * @param hasContainers
     * @return KeyRecord iterator for Edges attached to this Vertex.
     */
    public Iterator<KeyRecord> getEdgeKeyRecordsByIndex(final Direction direction,
                                                        final Set<String> labels,
                                                        final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                        final List<HasContainer> hasContainers) {
        return getEdgeKeyRecordsByIndex(direction, labels, outputType, hasContainers, null);
    }

    private Iterator<KeyRecord> getEdgeKeyRecordsByIndex(final Direction direction,
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
                id.getKeyHashString(), adjacentVertexId, direction);
        if (direction == Direction.OUT) {
            return new CachedIterator(graph, GraphQuery.create(graph).querySIndex(db.EDGE_AERO_SET, db.E_OUT_INDEX_NAME,
                    Filter.contains(db.SUPERNODES_OUT_BIN, IndexCollectionType.MAPVALUES, id.getKeyHashString()),
                    queryPolicy));
        } else if (direction == Direction.IN) {
            return new CachedIterator(graph, GraphQuery.create(graph).querySIndex(db.EDGE_AERO_SET, db.E_IN_INDEX_NAME,
                    Filter.contains(db.SUPERNODES_IN_BIN, IndexCollectionType.MAPVALUES, id.getKeyHashString()),
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

    public long getEdgeCount(final Direction direction) {
        if (direction == Direction.BOTH) {
            LOG.warn("getEdgeCount invoked with direction BOTH - the return value will be correct, but this method " +
                    "is only supposed to be invoked by FireflyVertexLocalCountStep which should never pass in BOTH.");
            return getEdgeCount(Direction.IN) + getEdgeCount(Direction.OUT);
        }

        final long baseCount = getCachedEdgeCount(direction);
        return this.isEdgeCacheOverflowed ?
                baseCount + FireflyCloseableIteratorUtils.count(getSupernodeEdgeIds(direction, Set.of(), Collections.emptyList())) :
                baseCount;
    }

    private long getCachedEdgeCount(final Direction direction) {
        final Map<String, List<LazyIdTransform>> edgeCache = direction == Direction.IN ? this.inEdgeIds : this.outEdgeIds;
        long size = 0;
        for (final List<LazyIdTransform> ids : edgeCache.values()) {
            size += ids.size();
        }
        return size;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        final Set<String> edgeLabelSet = new HashSet<>(Arrays.asList(edgeLabels));
        return getVerticesFromVertex(direction, edgeLabelSet);
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
        boolean includeSupernodeVirtualProperty = false;
        if (propertyKeys.length > 1) {
            for (final String propertyKey : propertyKeys) {
                if (propertyKey.equals(SUPERNODE_PROPERTY_KEY)) {
                    includeSupernodeVirtualProperty = true;
                    break;
                }
            }
        }
        // Read multiple vertex properties.
        final Iterator<Map.Entry<String, VertexProperty<V>>> vertexProperties = readVertexProperties(includeSupernodeVirtualProperty);
        // Return an iterator over the map.
        Iterator<VertexProperty<V>> iterator = (!vertexProperties.hasNext()) ? Collections.emptyIterator() :
                FireflyCloseableIteratorUtils.map(FireflyCloseableIteratorUtils.filter(vertexProperties,
                                e -> ElementHelper.keyExists(e.getKey(), propertyKeys)),
                        Map.Entry::getValue);

        if (!FireflyHelper.inComputerMode(this.graph))
            return iterator;
        else {
            // TODO: GRAPH COMPUTER INTERCEPTION
            final LocalGraphComputerView view = FireflyHelper.getGraphComputerView(this.graph);
            final List<VertexProperty<V>> computeProperties = view.getComputeProperties(this, propertyKeys);
            return (computeProperties.isEmpty()) ? iterator : FireflyCloseableIteratorUtils.concat(iterator, computeProperties.iterator());
        }
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
     * @param vertexId              Id of vertex.
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
                validProperties.add(property);
            }
        }

        final List<Operation> operations = new ArrayList<>();
        long ttlValueLong = 0;
        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {
            final PropertyValueIdMaps propertyValueIdMaps = getPropertyValueIdMaps(graph, validProperties);
            // Handle special TTL property if flag is enabled.
            if (propertyValueIdMaps.valueMap.containsKey(TTL_PROPERTY_KEY)) {
                if (!db.TTL_ENABLED_FLAG) {
                    throw new TtlNotEnabledException();
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
            graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, FireflyVertexProperty.class);
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

    public static List<FireflyVertex> readVertices(final FireflyGraph graph,
                                                   final ReadInfo readInfo) {
        LOG.debug("Reading vertices {}.", readInfo.ids);

        // Get database connection.
        final AerospikeConnection db = graph.getBaseGraph();

        // Batch read vertex records.
        final List<FireflyRecord> vertexRecords = FireflyRecord.batchRead(db, readInfo);
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

        // Get inEdgeIds and outEdgeIds.
        final Map<String, List<Object>> inEdgeIds = (Map) record.getMap(db.IN_EDGES_BIN);
        graph.getIdFactory().convertMapToLazyIdsInPlace(inEdgeIds, graph, FireflyVertex.class);
        final Map<String, List<Object>> outEdgeIds = (Map) record.getMap(db.OUT_EDGES_BIN);
        graph.getIdFactory().convertMapToLazyIdsInPlace(outEdgeIds, graph, FireflyVertex.class);
        final Map<String, List<LazyIdTransform>> fireflyInEdgeIds = (Map) inEdgeIds;
        final Map<String, List<LazyIdTransform>> fireflyOutEdgeIds = (Map) outEdgeIds;
        final Map<Object, Map<String, Object>> vertexPropertyProperties = (Map) record.getMap(db.PROPERTIES_BIN);
        final Map<Object, Map<String, Object>> vertexPropertyPropertiesTypeHints = (Map) record.getMap(db.TYPE_HINTS_BIN);


        // Create vertex based on type hint.
        if (vertexTypeHint == FireflyVertex.VERTEX_TYPE_HINT) {// Get vertex properties and vertex property counter from record.
            final Map<String, Object> vertexPropertyValues =
                    (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
            final Map<String, Object> vertexPropertyTypeHints =
                    (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
            final Map<String, Object> vertexPropertyIds =
                    (Map<String, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID_BIN);
            graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, FireflyVertexProperty.class);
            final Map<String, LazyIdTransform> fireflyVertexPropertyIds = (Map) vertexPropertyIds;
            return FireflyVertexFactory.create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                    fireflyVertexPropertyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyProperties,
                    vertexPropertyPropertiesTypeHints,
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

    public static class FireflyVertexFactory {
        public static FireflyVertex create(final FireflyId fid,
                                           final String label,
                                           final FireflyGraph graph,
                                           final Map<String, List<LazyIdTransform>> inEdgeIds,
                                           final Map<String, List<LazyIdTransform>> outEdgeIds,
                                           final Map<String, LazyIdTransform> vertexPropertyIds,
                                           final Map<String, Object> vertexPropertyValues,
                                           final Map<String, Object> vertexPropertyValuesTypeHints,
                                           final Map<Object, Map<String, Object>> vertexPropertyProperties,
                                           final Map<Object, Map<String, Object>> vertexPropertyPropertiesTypeHints,
                                           final boolean isEdgeCacheOverflowed,
                                           final AerospikeConnection db) {

            return new FireflyVertex(
                    fid,
                    label,
                    graph,
                    inEdgeIds,
                    outEdgeIds,
                    vertexPropertyIds,
                    vertexPropertyValues,
                    vertexPropertyValuesTypeHints,
                    vertexPropertyProperties,
                    vertexPropertyPropertiesTypeHints,
                    isEdgeCacheOverflowed,
                    db);
        }
    }
}
