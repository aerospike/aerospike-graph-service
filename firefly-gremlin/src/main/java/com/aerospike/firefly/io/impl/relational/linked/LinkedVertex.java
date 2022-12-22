package com.aerospike.firefly.io.impl.relational.linked;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import groovy.util.MapEntry;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedTypeValues;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.utils.GenerationCheck.RECORD_TOO_BIG_ERROR;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class LinkedVertex extends RelationalVertex {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedVertex.class);
    public static final int VERTEX_TYPE_HINT = 0;

    private Map<String, List<FireflyId>> vertexPropertyIds;
    private boolean isVertexPropertyCacheDisabled;

    /**
     * Constructor for LinkedVertex.
     *
     * @param fid                           firefly id.
     * @param label                         label.
     * @param graph                         graph.
     * @param inEdgeIds                     incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds                    outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount                   incoming edge count.
     * @param outEdgeCount                  outgoing edge count.
     * @param vertexPropertyIds             vertex property ids.
     * @param isVertexPropertyCacheDisabled is vertex property cache disabled.
     * @param isEdgeCacheDisabled           is edge cache disabled.
     * @param db                            Aerospike connection.
     */
    public LinkedVertex(final FireflyId fid,
                        final String label,
                        final FireflyGraph graph,
                        final Map<String, List<FireflyId>> inEdgeIds,
                        final Map<String, List<FireflyId>> outEdgeIds,
                        final long inEdgeCount,
                        final long outEdgeCount,
                        final Map<String, List<FireflyId>> vertexPropertyIds,
                        final boolean isVertexPropertyCacheDisabled,
                        final boolean isEdgeCacheDisabled,
                        final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, isEdgeCacheDisabled, db);
        this.vertexPropertyIds = vertexPropertyIds == null ? new TreeMap<>() : vertexPropertyIds;
        this.isVertexPropertyCacheDisabled = isVertexPropertyCacheDisabled;
    }

    @Override
    protected void removeVertexProperties() {
        // Remove vertex properties.
        final Set<Map.Entry<String, List<FireflyId>>> vertexPropertyIdMap;
        if (!this.isVertexPropertyCacheDisabled) {
            vertexPropertyIdMap = this.vertexPropertyIds.entrySet();
        } else {
            Map<String, FireflyVertexProperty<?>> vertexProperties = readVertexPropertiesByScan();
            vertexPropertyIdMap = new HashSet<>();
            for (Map.Entry<String, FireflyVertexProperty<?>> entry : vertexProperties.entrySet()) {
                vertexPropertyIdMap.add(new AbstractMap.SimpleEntry<>(entry.getKey(), new ArrayList<>() {{
                    add(entry.getValue().id);
                }}));
            }
        }

        vertexPropertyIdMap.forEach(entry -> {
            // Note, use LinkedVertexProperty.removeVertexProperty() function because it negates trying to remove
            // the vertex property from the vertex.
            final List<FireflyId> vertexPropertyIdList = entry.getValue();
            vertexPropertyIdList.forEach(id -> LinkedVertexProperty.removeVertexProperty(graph, FireflyIdFactory.createId(id)));
        });

        // This isn't exactly necessary since this Vertex is going to be removed.
        this.vertexPropertyIds = Collections.emptyMap();
        this.isVertexPropertyCacheDisabled = false;
    }

    /**
     * Read the vertex properties for the associated Vertex.
     * the vertex record has a Map[String,List[ID]] inside it.
     * from this each VertexProperty is read from its own record by id.
     *
     * @return Map of label to list of VertexProperty
     */
    public Map<String, FireflyVertexProperty<?>> readVertexPropertiesByScan() {
        final Long storageId = (Long) id.getStorageId();
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(db.PARENT_VERTEX_ID),
                        Exp.val(storageId))
        );

        // Create scan policy, do not need bin data for this.
        final ScanPolicy policy = new ScanPolicy();
        policy.includeBinData = false;

        // Scan vertex property set.
        final Iterator<Map.Entry<Key, Record>> records = scanAllRecordsInSet(db.VERTEX_PROPERTY_AERO_SET, exp, policy);

        final Map<String, FireflyVertexProperty<?>> results = new TreeMap<>();
        records.forEachRemaining(entry -> {
            final FireflyRecord fireflyRecord = FireflyRecord.fromRecord(db, entry.getKey(), entry.getValue());
            final FireflyId fid = FireflyIdFactory.createFromRecord(db, fireflyRecord);
            final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(
                    db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, fid, db.KEY_VALUE));
            final FireflyVertexProperty<?> vp = (kv.isEmpty()) ?
                    null :
                    new LinkedVertexProperty(graph, fid, this, kv.get().getKey(), kv.get().getValue());
            results.put(vp.label(), vp);
        });
        return results;
    }

    /**
     * Get the vertex property by vertex property label for the vertex using a scan.
     *
     * @param key vertex property label.
     * @return Iterator of FireflyVertexProperty for provided vertex property label.
     */
    private <V> Iterator<VertexProperty<V>> getVertexPropertyByScan(final String key) {
        LOG.debug("Getting vertex property by scan for {}", key);
        final Long storageId = (Long) this.id.getStorageId();
        final Expression exp = Exp.build(
                Exp.and(
                        Exp.eq(
                                Exp.intBin(this.db.PARENT_VERTEX_ID),
                                Exp.val(storageId)
                        ),
                        Exp.eq(
                                Exp.stringBin(this.db.VERTEX_PROPERTY_NAME),
                                Exp.val(key)
                        )
                )
        );

        // Scan vertex property set.
        final Iterator<Map.Entry<Key, Record>> records = scanAllRecordsInSet(db.VERTEX_PROPERTY_AERO_SET, exp,
                new ScanPolicy());

        final List<VertexProperty<?>> results = new ArrayList<>();
        records.forEachRemaining(entry -> {
            final FireflyRecord fireflyRecord = FireflyRecord.fromRecord(db, entry.getKey(), entry.getValue());
            final FireflyId fid = FireflyIdFactory.createFromRecord(db, fireflyRecord);
            final Long typeHint = (Long) entry.getValue().getMap(db.TYPE_HINTS).get(key);
            final Object value = db.convertValuetoTypeUsingHint(entry.getValue().getMap(db.KEY_VALUE).get(key), typeHint);
            results.add(new LinkedVertexProperty<>(graph, fid, this, key, value));
        });
        return IteratorUtils.asIterator(results);
    }

    /**
     * Remove the vertex property from the vertex.
     *
     * @param key              vertex property label.
     * @param vertexPropertyId id of the vertex property to remove.
     */
    @Override
    public void removeVertexPropertyForModel(final String key, final FireflyId vertexPropertyId) {
        // Cache is already disabled for this vertex and caches are never re-enabled.
        // Don't need to do anything since the cache is no longer used.
        if (this.isVertexPropertyCacheDisabled) {
            return;
        }

        // Get key for this vertex in database.
        final Key vertexKey = getKey(this.db.getNamespace(), this.db.VERTEX_AERO_SET, this.id);

        // Create operations for removing a vertex property.
        final Bin vpCounter = new Bin(this.db.VP_COUNTER, -1L);
        final Operation getCacheDisabled = Operation.get(this.db.VP_CACHE_DISABLED);
        final Operation decrementVpCount = Operation.add(vpCounter);
        final Operation removeVpId = ListOperation.removeByValue(
                this.db.VERTEX_PROPERTY_NAME_TO_ID,
                Value.get(vertexPropertyId.getStorageId()),
                ListReturnType.NONE,
                CTX.mapKeyCreate(Value.get(key), MapOrder.KEY_ORDERED)
        );
        final Operation removeEmptyVpKeys = MapOperation.removeByValue(this.db.VERTEX_PROPERTY_NAME_TO_ID,
                Value.get(new ArrayList<>()), MapReturnType.NONE);

        // Operate on database.
        final Record results = this.db.getClient().operate(null, vertexKey, getCacheDisabled, decrementVpCount,
                removeVpId, removeEmptyVpKeys);

        final boolean cacheDisabled = results.getBoolean(this.db.VP_CACHE_DISABLED);

        if (cacheDisabled) {
            // Cache was disabled by a different concurrent traversal.

            // Don't need to do anything more - just update the cache disabled flag of this.
            this.isVertexPropertyCacheDisabled = true;
        } else {
            // The cache is still enabled so just update the cache of this.
            if (!this.vertexPropertyIds.containsKey(key)) {
                LOG.error("Could not find vertex property {} in vertex {}. Vertex properties did not contain key {}.",
                        vertexPropertyId, this.id, key);
                return;
            }

            final List<FireflyId> vertexPropertyIdsForKey = this.vertexPropertyIds.get(key);
            if (!vertexPropertyIdsForKey.contains(vertexPropertyId)) {
                LOG.error("Could not find vertex property {} in vertex {}. Vertex properties under key {} did not contain vertex property {}.",
                        vertexPropertyId, this.id, key, vertexPropertyId);

            }

            // Remove item from vertex property Map.
            vertexPropertyIdsForKey.remove(vertexPropertyId);

            // Remove key if IDs are empty.
            if (vertexPropertyIdsForKey.isEmpty()) {
                this.vertexPropertyIds.remove(key);
            }
        }
    }

    /**
     * Read vertex properties for the vertex.
     *
     * @return Iterator of String label to List of FireflyVertexProperty
     */
    @Override
    protected <V> Iterator<Map.Entry<String, VertexProperty<V>>> readVertexProperties() {
        LOG.debug("Read vertex properties");

        // If vertex properties are not cached, read them from scan.
        if (this.isVertexPropertyCacheDisabled) {
            return IteratorUtils.asIterator(readVertexPropertiesByScan());
        }

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final List<Map.Entry<String, FireflyVertexProperty<V>>> vertexProperties = new ArrayList<>();
        for (final Map.Entry<String, List<FireflyId>> vertexPropertyIdsEntry : this.vertexPropertyIds.entrySet()) {
            // Get the properties for the entry.
            vertexPropertyIdsEntry.getValue().forEach(id -> {
                // Create the property.
                final FireflyVertexProperty<V> property = LinkedVertexProperty.readVertexProperty(graph, this, id);
                vertexProperties.add(new MapEntry(vertexPropertyIdsEntry.getKey(), property));
            });

        }

        return IteratorUtils.asIterator(vertexProperties);
    }

    /**
     * Get the vertex property by vertex property label for the vertex.
     *
     * @param key vertex property label.
     * @return Iterator of FireflyVertexProperty for provided vertex property label.
     */
    @Override
    protected <V> Iterator<VertexProperty<V>> readVertexProperty(final String key) {
        LOG.debug("Read vertex property {}", key);

        if (this.isVertexPropertyCacheDisabled) {
            return getVertexPropertyByScan(key);
        } else if (!vertexPropertyIds.containsKey(key)) {
            return Collections.emptyIterator();
        }

        final FireflyRecord record = FireflyRecord.read(db, db.VERTEX_AERO_SET, this.id);
        final List<FireflyId> vertexPropertyIdList;
        if (record != null) {
            final List<Object> ids = (List<Object>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID).get(key);
            vertexPropertyIdList = FireflyIdFactory.convertObjectListToFireflyIdList(ids);
        } else {
            vertexPropertyIdList = vertexPropertyIds.get(key);
        }
        final List<FireflyVertexProperty<?>> vertexProperties = new ArrayList<>();
        vertexPropertyIdList.forEach(vertexPropertyId -> {
            final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(
                    db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, vertexPropertyId, db.KEY_VALUE));
            final FireflyVertexProperty<?> vertexProperty = (kv.isEmpty()) ?
                    null :
                    new LinkedVertexProperty<V>(graph, vertexPropertyId, this, kv.get().getKey(), kv.get().getValue());
            if (vertexProperty != null)
                vertexProperties.add(vertexProperty);
        });
        return IteratorUtils.asIterator(vertexProperties);
    }

    /**
     * Write vertex property to vertex.
     *
     * @param vertexProperty Vertex property to write to vertex.
     */
    @Override
    public void writeVertexProperty(final FireflyVertexProperty vertexProperty) {
        // Cache is already disabled for this vertex and caches are never re-enabled.
        // Don't need to do anything since the cache is no longer used.
        if (this.isVertexPropertyCacheDisabled) {
            return;
        }

        // Get key for this vertex in database.
        final Key key = getKey(this.db.getNamespace(), this.db.VERTEX_AERO_SET, this.id);

        // Create operations for writing a vertex property.
        final Bin vpCounter = new Bin(this.db.VP_COUNTER, 1);
        final Operation getCacheDisabled = Operation.get(this.db.VP_CACHE_DISABLED);
        final Operation incrementVpCount = Operation.add(vpCounter);
        final Operation getVpCount = Operation.get(this.db.VP_COUNTER);
        final Operation appendVpId = ListOperation.append(
                this.db.VERTEX_PROPERTY_NAME_TO_ID,
                Value.get(vertexProperty.id.getStorageId()),
                CTX.mapKeyCreate(Value.get(vertexProperty.key()), MapOrder.KEY_ORDERED)
        );

        // Operate on database.
        final Record results;
        try {
            results = this.db.getClient().operate(null, key, getCacheDisabled, incrementVpCount,
                    getVpCount, appendVpId);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.RECORD_TOO_BIG)
                throw new RuntimeException(RECORD_TOO_BIG_ERROR);
            throw ae;
        }

        final boolean cacheDisabled = results.getBoolean(this.db.VP_CACHE_DISABLED);
        final long vpCount = results.getLong(this.db.VP_COUNTER);

        if (cacheDisabled) {
            // Cache was disabled by a different concurrent traversal.

            // Don't need to do anything more - just update the cache disabled flag of this.
            this.isVertexPropertyCacheDisabled = true;
        } else if (vpCount > this.db.ID_CACHE_SIZE) {
            // The incremented vertex property count triggers the threshold for disabling the cache.

            // Disable the cache for this vertex in database.
            final Bin vertexPropertyCacheDisabledBin = new Bin(this.db.VP_CACHE_DISABLED, true);
            final Operation disableCache = Operation.put(vertexPropertyCacheDisabledBin);
            // Wipe vertex property cache for this vertex in database.
            final Bin vertexPropertyIdBin =
                    new Bin(this.db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(new TreeMap<String, List<Object>>()));
            final Operation wipeCache = Operation.put(vertexPropertyIdBin);
            this.db.getClient().operate(null, key, disableCache, wipeCache);

            // Update cache disabled flag of this.
            this.isVertexPropertyCacheDisabled = true;
        } else {
            // The cache is still enabled so just update the cache of this.
            if (!this.vertexPropertyIds.containsKey(vertexProperty.key())) {
                this.vertexPropertyIds.put(vertexProperty.key(), new ArrayList<>());
            }
            this.vertexPropertyIds.get(vertexProperty.key()).add(vertexProperty.id);
        }
    }

    /**
     * Read vertex property keys.
     *
     * @return Set of vertex property keys.
     */
    @Override
    protected Set<String> readVertexPropertyKeys() {
        return (this.isVertexPropertyCacheDisabled) ?
                readVertexPropertiesByScan().keySet() :
                vertexPropertyIds.keySet();
    }
}
