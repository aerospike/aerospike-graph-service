package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import groovy.util.MapEntry;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.aerospike.firefly.io.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.utils.OperationReturnHandler.getValueAtIndex;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class PackedVertex extends RelationalVertex {
    private static final Logger LOG = LoggerFactory.getLogger(PackedVertex.class);
    public static final int VERTEX_TYPE_HINT = 1;

    private Map<String, FireflyId> vertexPropertyIds;
    private Map<String, Object> vertexPropertyValues;
    private Map<String, Long> vertexPropertyValuesTypeHints;
    private Map<Object, Map<String, Object>> vertexPropertyIdToProperties;
    private Map<Object, Map<String, Long>> vertexPropertyIdToTypeHints;

    /**
     * Constructor for PackedVertex.
     *
     * @param fid                           firefly id.
     * @param label                         label.
     * @param graph                         graph.
     * @param inEdgeIds                     incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds                    outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount                   incoming edge count.
     * @param outEdgeCount                  outgoing edge count.
     * @param vertexPropertyIds             vertex property ids.
     * @param vertexPropertyValues          vertex property values.
     * @param vertexPropertyValuesTypeHints vertex property value type hints.
     * @param isEdgeCacheOverflowed         is the edge cache overflowed.
     * @param db                            Aerospike connection.
     */
    protected PackedVertex(final FireflyId fid,
                           final String label,
                           final FireflyGraph graph,
                           final Map<String, List<FireflyId>> inEdgeIds,
                           final Map<String, List<FireflyId>> outEdgeIds,
                           final long inEdgeCount,
                           final long outEdgeCount,
                           final Map<String, FireflyId> vertexPropertyIds,
                           final Map<String, Object> vertexPropertyValues,
                           final Map<String, Long> vertexPropertyValuesTypeHints,
                           final Map<Object, Map<String, Object>> vertexPropertyIdToProperties,
                           final Map<Object, Map<String, Long>> vertexPropertyIdToTypeHints,
                           final boolean isEdgeCacheOverflowed,
                           final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, isEdgeCacheOverflowed, db);

        // To enable values to have index functions run, cardinality must be single.
        if (graph().features().vertex().getCardinality("") != VertexProperty.Cardinality.single) {
            throw new RuntimeException("PackedVertex only supports for single cardinality");
        }

        this.vertexPropertyIds = vertexPropertyIds == null ? new TreeMap<>() : vertexPropertyIds;
        this.vertexPropertyValues = vertexPropertyValues == null ? new TreeMap<>() : vertexPropertyValues;
        this.vertexPropertyValuesTypeHints = vertexPropertyValuesTypeHints == null ? new TreeMap<>() : vertexPropertyValuesTypeHints;
        this.vertexPropertyIdToProperties = vertexPropertyIdToProperties == null ? new TreeMap<>() : vertexPropertyIdToProperties;
        this.vertexPropertyIdToTypeHints = vertexPropertyIdToTypeHints == null ? new TreeMap<>() : vertexPropertyIdToTypeHints;
    }

    @Override
    protected void removeVertexProperties() {
        vertexPropertyIds = new TreeMap<>();
        vertexPropertyValues = new TreeMap<>();
        vertexPropertyValuesTypeHints = new TreeMap<>();
    }

    /**
     * Read vertex properties for the vertex.
     *
     * @return Iterator of String label to List of FireflyVertexProperty
     */
    @Override
    protected <V> Iterator<Map.Entry<String, VertexProperty<V>>> readVertexProperties() {
        LOG.debug("Read vertex properties");

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final List<Map.Entry<String, FireflyVertexProperty<V>>> vertexPropertyList = new ArrayList<>();

        for (final Map.Entry<String, Object> vertexProperty : vertexPropertyValues.entrySet()) {
            final String vpKey = vertexProperty.getKey();
            final Object vpValue = this.db.convertValuetoTypeUsingHint(vertexPropertyValues.get(vpKey),
                    vertexPropertyValuesTypeHints.get(vpKey));
            final FireflyId vpId = graph.getIdFactory().createId(vertexPropertyIds.get(vertexProperty.getKey()), FireflyVertexProperty.class);
            final Map<String, Object> vpProperties = vertexPropertyIdToProperties.containsKey(vpId.getStorageId()) ?
                    vertexPropertyIdToProperties.get(vpId.getStorageId()) : new TreeMap<>();
            final Map<String, Long> vpTypeHints = vertexPropertyIdToTypeHints.containsKey(vpId.getStorageId()) ?
                    vertexPropertyIdToTypeHints.get(vpId.getStorageId()) : new TreeMap<>();

            // Create the property.
            final FireflyVertexProperty<V> property = new PackedVertexProperty<>(graph,
                    graph.getIdFactory().createId(vertexPropertyIds.get(vertexProperty.getKey()), FireflyVertexProperty.class),
                    this, vpKey, vpValue, vpProperties, vpTypeHints);
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
    @Override
    protected <V> Iterator<VertexProperty<V>> readVertexProperty(final String key) {
        LOG.debug("Reading vertex property {}", key);

        if (!vertexPropertyValues.containsKey(key)) {
            return Collections.emptyIterator();
        }

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final Object vertexProperty = this.db.convertValuetoTypeUsingHint(vertexPropertyValues.get(key),
                vertexPropertyValuesTypeHints.get(key));
        final FireflyId vertexPropertyId = vertexPropertyIds.get(key);
        final Map<String, Object> vpProperties = vertexPropertyIdToProperties.containsKey(vertexPropertyId.getStorageId()) ?
                vertexPropertyIdToProperties.get(vertexPropertyId.getStorageId()) : new TreeMap<>();
        final Map<String, Long> vpTypeHints = vertexPropertyIdToTypeHints.containsKey(vertexPropertyId.getStorageId()) ?
                vertexPropertyIdToTypeHints.get(vertexPropertyId.getStorageId()) : new TreeMap<>();
        return FireflyCloseableIteratorUtils.of(
                new PackedVertexProperty<>(graph, vertexPropertyId, this, key, vertexProperty, vpProperties, vpTypeHints));
    }

    /**
     * Remove the vertex property from the vertex.
     *
     * @param key              vertex property label.
     * @param vertexPropertyId id of the vertex property to remove.
     */
    @Override
    public void removeVertexPropertyForModel(final String key, final FireflyId vertexPropertyId) {
        final Key opKey = getKey(this.db, this.db.VERTEX_AERO_SET, this.id);

        // Remove Vertex Property's Properties.
        final Operation removeProperty =
                MapOperation.removeByKey(this.db.PROPERTIES, Value.get(vertexPropertyId.getStorageId()), MapReturnType.NONE);
        final Operation removePropertyTypeHint =
                MapOperation.removeByKey(this.db.TYPE_HINTS, Value.get(vertexPropertyId.getStorageId()), MapReturnType.NONE);

        // Remove Vertex Property.
        final Operation removeVertexPropertyValue =
                MapOperation.removeByKey(this.db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(key), MapReturnType.NONE);
        final Operation removeVertexPropertyTypeHint =
                MapOperation.removeByKey(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(key), MapReturnType.NONE);
        final Operation removeVertexPropertyId =
                MapOperation.removeByKey(this.db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(key), MapReturnType.NONE);

        final Operation getVertexPropertyValues = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE);
        final Operation getVertexPropertyValuesTypeHints =
                Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
        final Operation getVertexPropertyIds = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_ID);
        final Operation getVertexPropertyProperties = Operation.get(this.db.PROPERTIES);
        final Operation getVertexPropertyTypeHints = Operation.get(this.db.TYPE_HINTS);

        final FireflyCache cache = this.db.transactionCache.get();
        if (cache != null) {
            cache.invalidate(opKey);
        }
        final Record result = this.db.operate(null, opKey, removeProperty, removePropertyTypeHint,
                removeVertexPropertyValue, removeVertexPropertyId, removeVertexPropertyTypeHint,
                getVertexPropertyValues, getVertexPropertyValuesTypeHints, getVertexPropertyIds,
                getVertexPropertyProperties, getVertexPropertyTypeHints);

        final Map<String, Object> vertexPropertyValues =
                (Map<String, Object>) getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE, 1);
        final Map<String, Long> vertexPropertyValuesTypeHints =
                (Map<String, Long>) getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, 1);
        final Map<String, Object> vertexPropertyIds =
                (Map<String, Object>) getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_ID, 1);
        final Map<Object, Map<String, Object>> vertexPropertyIdToProperties =
                (Map<Object, Map<String, Object>>) getValueAtIndex(result, this.db.PROPERTIES, 1);
        final Map<Object, Map<String, Long>> vertexPropertyIdToTypeHints =
                (Map<Object, Map<String, Long>>) getValueAtIndex(result, this.db.TYPE_HINTS, 1);
        final Map<String, FireflyId> vertexPropertyFireflyIds =
                this.graph.getIdFactory().convertMapObjectToFireflyIdMap(vertexPropertyIds, FireflyVertexProperty.class);

        // Update this PackedVertex in JVM cache
        updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues,
                vertexPropertyValuesTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
    }

    private void updateVertexPropertyJVMCache(final Map<String, FireflyId> vertexPropertyIds,
                                              final Map<String, Object> vertexPropertyValues,
                                              final Map<String, Long> vertexPropertyValuesTypeHints,
                                              final Map<Object, Map<String, Object>> vertexPropertyIdToProperties,
                                              final Map<Object, Map<String, Long>> vertexPropertyIdToTypeHints) {
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
    @Override
    public void writeVertexProperty(final FireflyVertexProperty vertexProperty) {
        final Key key = getKey(this.db, this.db.VERTEX_AERO_SET, this.id);

        final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation putValue = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_VALUE,
                Value.get(vertexProperty.key()), Value.get(vertexProperty.value()));
        final Operation putTypeHint = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT,
                Value.get(vertexProperty.key()), Value.get(getSupportedType(vertexProperty.value().getClass())));
        final Operation putId = MapOperation.put(policy, this.db.VERTEX_PROPERTY_NAME_TO_ID,
                Value.get(vertexProperty.key()), Value.get(vertexProperty.id.getStorageId()));
        final Operation getValues = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE);
        final Operation getTypeHints = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
        final Operation getIds = Operation.get(this.db.VERTEX_PROPERTY_NAME_TO_ID);

        // Write key for the vertex property's properties
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation addKeyProperties = MapOperation.put(mapPolicy, this.db.PROPERTIES,
                Value.get(vertexProperty.id.getStorageId()), Value.get(vertexProperty.properties));
        final Operation addKeyPropertiesTypeHints = MapOperation.put(mapPolicy, this.db.TYPE_HINTS,
                Value.get(vertexProperty.id.getStorageId()), Value.get(vertexProperty.typeHints));
        final Operation getKeyProperties = Operation.get(this.db.PROPERTIES);
        final Operation getKeyPropertiesTypeHints = Operation.get(this.db.TYPE_HINTS);

        final FireflyCache cache = db.transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        final Record result = this.db.operate(writePolicy, key, putValue, putId, putTypeHint, addKeyProperties, addKeyPropertiesTypeHints, getValues, getTypeHints,
                getIds, getKeyProperties, getKeyPropertiesTypeHints);

        final Map<String, Object> vertexPropertyValues = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE, 1)).orElse(new TreeMap<>());
        final Map<String, Long> vertexPropertyTypeHints = (Map<String, Long>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, 1)).orElse(new TreeMap<>());
        final Map<String, Object> vertexPropertyIds = (Map<String, Object>) Optional.ofNullable(getValueAtIndex(result, this.db.VERTEX_PROPERTY_NAME_TO_ID, 1)).orElse(new TreeMap<>());
        final Map<Object, Map<String, Object>> vertexPropertyIdToProperties = (Map<Object, Map<String, Object>>) Optional.ofNullable(getValueAtIndex(result, this.db.PROPERTIES, 1)).orElse(new TreeMap<>());
        final Map<Object, Map<String, Long>> vertexPropertyIdToTypeHints = (Map<Object, Map<String, Long>>) Optional.ofNullable(getValueAtIndex(result, this.db.TYPE_HINTS, 1)).orElse(new TreeMap<>());
        final Map<String, FireflyId> vertexPropertyFireflyIds = this.graph.getIdFactory().convertMapObjectToFireflyIdMap(vertexPropertyIds, FireflyVertexProperty.class);

        // Update this PackedVertex in JVM cache
        updateVertexPropertyJVMCache(vertexPropertyFireflyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);
        graph.fireflySummaryUpdater.addVertexPropertiesWriteToQueue(label, Set.of(vertexProperty.key()));
    }

    /**
     * Read vertex property keys.
     *
     * @return Set of vertex property keys.
     */
    @Override
    protected Set<String> readVertexPropertyKeys() {
        return vertexPropertyValues.keySet();
    }

    public static class PackedVertexFactory {
        public static PackedVertex create(final FireflyId fid,
                                          final String label,
                                          final FireflyGraph graph,
                                          final Map<String, List<FireflyId>> inEdgeIds,
                                          final Map<String, List<FireflyId>> outEdgeIds,
                                          final long inEdgeCount,
                                          final long outEdgeCount,
                                          final Map<String, FireflyId> vertexPropertyIds,
                                          final Map<String, Object> vertexPropertyValues,
                                          final Map<String, Long> vertexPropertyValuesTypeHints,
                                          final Map<Object, Map<String, Object>> vertexPropertyProperties,
                                          final Map<Object, Map<String, Long>> vertexPropertyPropertiesTypeHints,
                                          final boolean isEdgeCacheOverflowed,
                                          final AerospikeConnection db) {
            if (StarPackedGraph.isStarPackedGraph(graph)) {
                return new StarPackedVertex(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount,
                        vertexPropertyIds, vertexPropertyValues, vertexPropertyValuesTypeHints,
                        vertexPropertyProperties, vertexPropertyPropertiesTypeHints, isEdgeCacheOverflowed, db);
            } else {
                return new PackedVertex(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount,
                        vertexPropertyIds, vertexPropertyValues, vertexPropertyValuesTypeHints,
                        vertexPropertyProperties, vertexPropertyPropertiesTypeHints, isEdgeCacheOverflowed, db);
            }
        }
    }
}
