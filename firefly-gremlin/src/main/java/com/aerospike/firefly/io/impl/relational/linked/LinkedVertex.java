package com.aerospike.firefly.io.impl.relational.linked;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.io.utils.GenerationCheck;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import groovy.util.MapEntry;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class LinkedVertex extends RelationalVertex {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedVertex.class);
    public static final int VERTEX_TYPE_HINT = 0;

    private Map<String, List<FireflyId>> vertexPropertyIds;
    private long vertexPropertyCount;
    private AerospikeConnection db;

    /**
     * Constructor for LinkedVertex.
     *
     * @param fid                 firefly id.
     * @param label               label.
     * @param graph               graph.
     * @param inEdgeIds           incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds          outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount         incoming edge count.
     * @param outEdgeCount        outgoing edge count.
     * @param vertexPropertyIds   vertex property ids.
     * @param vertexPropertyCount vertex property count.
     * @param db                  Aerospike connection.
     */
    public LinkedVertex(final FireflyId fid,
                        final String label,
                        final FireflyGraph graph,
                        final Map<String, List<FireflyId>> inEdgeIds,
                        final Map<String, List<FireflyId>> outEdgeIds,
                        final long inEdgeCount,
                        final long outEdgeCount,
                        final Map<String, List<FireflyId>> vertexPropertyIds,
                        final long vertexPropertyCount,
                        final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, db);
        this.vertexPropertyCount = vertexPropertyCount;
        this.vertexPropertyIds = vertexPropertyIds == null ? new HashMap<>() : vertexPropertyIds;
        this.db = db;
    }

    @Override
    protected void removeVertexProperties() {
        // Remove vertex properties.
        final Set<Map.Entry<String, List<FireflyId>>> vertexPropertyIdMap = new HashSet<>(vertexPropertyIds.entrySet());
        vertexPropertyIdMap.forEach(entry -> {
            // Note, use LinkedVertexProperty.removeVertexProperty() function because it negates trying to remove
            // the vertex property from the vertex.
            final List<FireflyId> vertexPropertyIdList = entry.getValue();
            vertexPropertyIdList.forEach(id -> LinkedVertexProperty.removeVertexProperty(graph, FireflyIdFactory.createId(id)));
        });
        vertexPropertyIds = new HashMap<>();
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

        final Map<String, FireflyVertexProperty<?>> results = new HashMap<>();
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
        // TODO: Can we filter on key?
        LOG.debug("Getting vertex property by scan for {}", key);
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

        final List<VertexProperty<?>> results = new ArrayList<>();
        records.forEachRemaining(entry -> {
            if (entry.getKey().equals(key)) {
                final FireflyRecord fireflyRecord = FireflyRecord.fromRecord(db, entry.getKey(), entry.getValue());
                final FireflyId fid = FireflyIdFactory.createFromRecord(db, fireflyRecord);
                final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(
                        db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, fid, db.KEY_VALUE));
                final VertexProperty<?> vp = (kv.isEmpty()) ?
                        null :
                        new LinkedVertexProperty<V>(graph, fid, this, kv.get().getKey(), kv.get().getValue());
                results.add(vp);
            }
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
        GenerationCheck.writeGenerationCheck(() -> protectedRemoveVertexPropertyForModel(key, vertexPropertyId));
    }

    private void protectedRemoveVertexPropertyForModel(final String key, final FireflyId vertexPropertyId) {
        LOG.debug("Removing vertex property {} from vertex {}.", vertexPropertyId, id);

        // Read the vertex's firefly record from the database
        final FireflyRecord record = FireflyRecord.read(db, db.VERTEX_AERO_SET, id);
        if (record == null) {
            return;
        }

        // Read this vertex and update in case we have had concurrent updates.
        final LinkedVertex linkedVertex = (LinkedVertex) fromRecord(graph, new KeyRecord(record.key(), record.record()));
        this.vertexPropertyCount = linkedVertex.vertexPropertyCount;
        this.vertexPropertyIds = linkedVertex.vertexPropertyIds;

        if (!vertexPropertyIds.containsKey(key)) {
            LOG.error("Could not find vertex property {} in vertex {}. Vertex properties did not contain key {}.",
                      vertexPropertyId, id, key);
            return;
        }

        final List<FireflyId> vertexPropertyIdsForKey = vertexPropertyIds.get(key);
        if (vertexPropertyIdsForKey.contains(vertexPropertyId)) {
            vertexPropertyIdsForKey.remove(vertexPropertyId);
        } else {
            LOG.error("Could not find vertex property {} in vertex {}. Vertex properties under key {} did not contain vertex property {}.",
                      vertexPropertyId, id, key, vertexPropertyId);
            return;
        }

        // Remove item from vertex property Map.
        List<FireflyId> list = vertexPropertyIds.get(key);
        list.remove(vertexPropertyId);
        if (list.size() == 0) {
            vertexPropertyIds.remove(key);
        }

        vertexPropertyCount--;

        // Decrement counter
        final long vpCounter = getVertexPropertyCount() - vertexPropertyIdsForKey.size();

        // Write back.
        final int generation = record.record.generation;
        if (vpCounter <= db.ID_CACHE_SIZE - 1) {
            // Can directly overwrite vertex property map.
            final Bin vertexProperties = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(FireflyIdFactory.convertMapListToStorage(vertexPropertyIds)));
            final Bin vertexPropertiesCounter = new Bin(db.VP_COUNTER, Value.get(vpCounter));

            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, generation, vertexProperties, vertexPropertiesCounter);
        } else if (vpCounter > db.ID_CACHE_SIZE - 1) {
            // Can only overwrite vertex property count.
            final Bin vertexPropertiesCounter = new Bin(db.VP_COUNTER, Value.get(vpCounter));
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, generation, vertexPropertiesCounter);
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
        if (vertexPropertyIds == null) {
            return IteratorUtils.asIterator(readVertexPropertiesByScan());
        }

        // Vertex property ids are cached - loop through entries and get the properties for the entry.
        final List<Map.Entry<String, FireflyVertexProperty<V>>> vertexProperties = new ArrayList<>();
        final FireflyRecord record = FireflyRecord.read(db, db.VERTEX_AERO_SET, this.id);
        // The test case shouldRemoveMultiPropertiesWhenVerticesAreRemoved from the standard suite
        // Requires that the properties be read from the database, because they have been removed in a traversal
        final Map<String, List<FireflyId>> reReadIdMap;
        if (record != null) {
            final Map<String, List<Object>> vertexPropertyIds = (Map<String, List<Object>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
            reReadIdMap = FireflyIdFactory.convertMapListObjectToFireflyIdMap(vertexPropertyIds);
        } else {
            reReadIdMap = vertexPropertyIds;
        }
        for (final Map.Entry<String, List<FireflyId>> vertexPropertyIdsEntry : reReadIdMap.entrySet()) {
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

        if (vertexPropertyIds == null) {
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
        GenerationCheck.writeGenerationCheck(() -> protectedWriteVertexProperty(vertexProperty));
    }

    private void protectedWriteVertexProperty(final FireflyVertexProperty vertexProperty) {
        LOG.debug("Adding vertex property {} to vertex {}.", vertexProperty.id, id);
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, id);

        final Map<String, List<Object>> labelIds;
        if (fireflyRecord == null || fireflyRecord.record() == null) {
            labelIds = new HashMap<>();
        } else {
            labelIds = (Map<String, List<Object>>) Optional.ofNullable(
                    fireflyRecord.record().getMap(db.VERTEX_PROPERTY_NAME_TO_ID)).orElse(new HashMap<>());
        }

        long vpCounter = 0;
        int generation = -1;
        if (fireflyRecord != null && fireflyRecord.record() != null) {
            vpCounter = fireflyRecord.record().getLong(db.VP_COUNTER);
            generation = fireflyRecord.record.generation;
        }

        final List<Object> ids = labelIds.getOrDefault(vertexProperty.key(), new ArrayList<>());
        final List<FireflyId> fireflyIds = FireflyIdFactory.convertObjectListToFireflyIdList(ids);
        vertexPropertyIds.put(vertexProperty.key(), fireflyIds);
        vertexPropertyCount++;
        if (vpCounter < db.ID_CACHE_SIZE)
            ids.add(vertexProperty.id.getStorageId());
        vpCounter++;

        labelIds.put(vertexProperty.key(), ids);
        final Bin vertexPropertyIdBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(labelIds));
        final Bin vertexPropertyCountBin = new Bin(db.VP_COUNTER, Value.get(vpCounter));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, id, generation, vertexPropertyIdBin, vertexPropertyCountBin);
    }

    /**
     * Get vertex property count.
     *
     * @return Vertex property count.
     */
    @Override
    public long getVertexPropertyCount() {
        return IteratorUtils.count(readVertexProperties());
    }

    /**
     * Read vertex property keys.
     *
     * @return Set of vertex property keys.
     */
    @Override
    protected Set<String> readVertexPropertyKeys() {
        return (vertexPropertyIds == null) ?
                readVertexPropertiesByScan().keySet() :
                vertexPropertyIds.keySet();
    }
}
