package com.aerospike.firefly.io.impl.standard;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.AbstractBackend;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexPropertyBackend extends AbstractBackend implements Backend.VertexProperty {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);

    public VertexPropertyBackend(AerospikeConnection db) {
        super(db);
    }


    /**
     * Add a VertexProperty to a Vertex
     *
     * @param vertex
     * @param vp
     */
    @Override
    public void addVPToVertex(FireflyVertex vertex, FireflyVertexProperty vp) {
        LOG.debug("Adding vertex property {} to vertex {}.", vp, vertex);
        final FireflyRecord fireflyRecord = db.vertexBackend.getVertexRecord(vertex.id);

        Map<String, List<Long>> labelIds;
        if (fireflyRecord == null || fireflyRecord.record() == null) {
            labelIds = new HashMap<>();
        } else {
            labelIds = (Map<String, List<Long>>) Optional.ofNullable(fireflyRecord.record().getMap(db.VERTEX_PROPERTY_NAME_TO_ID)).orElse(new HashMap<>());
        }

        long vpCounter = 0;
        if (fireflyRecord != null && fireflyRecord.record() != null) {
            vpCounter = fireflyRecord.record().getLong(db.VP_COUNTER);
        }

        final List<Long> ids = labelIds.getOrDefault(vp.key(), new ArrayList<>());
        if (vpCounter < db.ID_CACHE_SIZE)
            ids.add(NumericIdManager.convert(vp.id()));
        vpCounter++;

        labelIds.put(vp.key(), ids);
        final Bin edgeData = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(labelIds));
        final Bin edgeCounterBin = new Bin(db.VP_COUNTER, Value.get(vpCounter));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertex.id, edgeData, edgeCounterBin);
    }

    /**
     * Read a single VertexProperty from its id
     *
     * @param parent Vertex that owns the VertexProperty being looked up
     * @param vpId   Id of VertexProperty to lookup
     * @param <V>    type
     * @return VertexProperty
     */
    @Override
    public <V> FireflyVertexProperty<V> readVertexProperty(final FireflyVertex parent, final FireflyId vpId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_PROPERTY_AERO_SET, vpId);
        if (fireflyRecord == null)
            throw new NoSuchElementException();
        return vertexPropertyFromRecord((FireflyGraph) parent.graph(), fireflyRecord, parent.id);
    }

    /**
     * Construct a VertexProperty object from a record
     *
     * @param fireflyRecord FireflyRecord with VertexProperty data
     * @param parentId      parent Vertex Id
     * @param <V>           type
     * @return FireflyVertexProperty
     */
    @Override
    public <V> FireflyVertexProperty<V> vertexPropertyFromRecord(FireflyGraph graph, FireflyRecord fireflyRecord, final FireflyId parentId) {
        FireflyId fid = FireflyId.of(FireflyVertexProperty.class, fireflyRecord.id());
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, fid, db.KEY_VALUE));
        if (kv.isEmpty())
            return new FireflyVertexProperty<V>(graph, fid, parentId, null, null);
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new FireflyVertexProperty<V>(graph, fid, parentId, vpKey, (V) vpVal);
    }


    /**
     * Read the struct of vertex properties for an associated Vertex
     * the vertex record has a Map[String,List[ID]] inside it.
     * from this each VertexProperty is read from its own record by id
     *
     * @param vertex parent Vertex
     * @return Map of label to list of VertexProperty
     */
    @Override
    public Map<String, List<VertexProperty>> readVertexPropertiesByScan(final FireflyVertex vertex) {
        final Object origId = vertex.id();
        final Long storageId = (Long) db.idToStorageType(origId);
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(db.PARENT_VERTEX_ID),
                        Exp.val(storageId))
        );
        final Iterator<Map.Entry<Key, Record>> records = db.scanAllRecordsInSet(db.VERTEX_PROPERTY_AERO_SET, exp);
        //all results, empty
        final Map<String, List<VertexProperty>> results = new HashMap<>();
        //for every vp id associated with vertex
        records.forEachRemaining(entry -> {
            //load the vp
            final VertexProperty<Object> vp = vertexPropertyFromRecord((FireflyGraph) vertex.graph(), FireflyRecord.fromRecord(db, entry.getKey(), entry.getValue()), vertex.id);
            //if there is a list for its key, get it, else, create it
            final List<VertexProperty> list = results.getOrDefault(vp.key(), new ArrayList<>());
            //add the vp to the list named for its key
            list.add(vp);
            //put the list back
            results.put(vp.key(), list);
        });
        return results;
    }

    /**
     * Return a single VertexProperty associated with a Vertex and key if vertex is in cache. Otherwise scan and return result.
     *
     * @param vertex parent Vertex
     * @return List of VertexProperty for provided key
     */
    @Override
    public List<VertexProperty> readVertexProperty(final FireflyVertex vertex, final String key) {
        final FireflyRecord r = db.vertexBackend.getVertexRecord(vertex.id);
        if (r == null) {
            return new ArrayList<>();
        }

        if (r.record.getLong(db.VP_COUNTER) < db.ID_CACHE_SIZE) {
            final Map<String, List<Long>> idMap = (Map<String, List<Long>>) r.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
            if (idMap == null) {
                return new ArrayList<>();
            }

            final List<Long> vp = idMap.getOrDefault(key, null);
            if (vp == null) {
                return new ArrayList<>();
            }

            final List<VertexProperty> vpList = new ArrayList<>();
            for (Long id : vp) {
                FireflyId fireflyId = FireflyId.of(FireflyVertexProperty.class, id);
                final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(db.readTypeHintedKeyValueFromMap(db.VERTEX_PROPERTY_AERO_SET, fireflyId, db.KEY_VALUE));
                if (kv.isEmpty())
                    vpList.add(new FireflyVertexProperty((FireflyGraph) vertex.graph(), fireflyId, vertex.id, null, null));
                else
                    vpList.add(new FireflyVertexProperty((FireflyGraph) vertex.graph(), fireflyId, vertex.id, kv.get().getKey(), kv.get().getValue()));
            }
            return vpList;
        } else {
            return readVertexPropertiesByScan(vertex).get(key);
        }
    }

    /**
     * return a map of VertexProperties associated with a Vertex
     *
     * @param vertex parenet Vertex
     * @return Map of label to List of VertexProperty
     */
    @Override
    public Map<String, List<VertexProperty>> readVertexProperties(final FireflyVertex vertex) {
        FireflyRecord r = db.vertexBackend.getVertexRecord(vertex.id);
        if (r == null) {
            return new HashMap<>();
        }
        long vp_count = r.record.getLong(db.VP_COUNTER);
        if (vp_count < db.ID_CACHE_SIZE) {
            final Map<String, List<Long>> idMap = db.vertexBackend.getXXXIdsFromVertexLabelMap(vertex, db.VERTEX_PROPERTY_NAME_TO_ID);
            final Map<String, List<VertexProperty>> vpLabelList = new HashMap<>();
            idMap.entrySet().forEach(entry -> {
                String label = entry.getKey();
                List<Long> idList = entry.getValue();
                List<VertexProperty> vpList = new ArrayList<>();
                idList.forEach(id -> {
                    vpList.add(readVertexProperty(vertex, FireflyId.of(FireflyVertexProperty.class, id)));
                });
                vpLabelList.put(label, vpList);
            });
            return vpLabelList;

        } else
            return readVertexPropertiesByScan(vertex);
    }


    /**
     * Write a new vertex property
     *
     * @param vertex parent Vertex
     * @param vpid   VertexProperty id to write
     * @param vpk    VP key
     * @param key    VP key
     * @param value  VP value
     * @param <V>    type
     */
    @Override
    public <V> void writeVertexProperty(final FireflyVertex vertex,
                                        final FireflyId vpid,
                                        final String vpk,
                                        final String key,
                                        final V value) {
        final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, vpk);
        final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, db.idToStorageType(vertex.id()));
        db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vpid, db.KEY_VALUE, key, value, vpkBin, pviBin);
        addVPToVertex(vertex, readVertexProperty(vertex, vpid));
    }

    /**
     * @param vertex Vertex to operate on
     * @param vp     VertexProperty to remove from Vertex id cache
     */
    @Override
    public void removeIdFromVertexPropertyList(final FireflyVertex vertex, final VertexProperty vp) {
        final FireflyRecord vertexRecord = db.vertexBackend.getVertexRecord(vertex.id);
        if (vertexRecord == null)
            throw new NoSuchElementException();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) vertexRecord.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        final List<Object> ids = propertyKeys.getOrDefault(vp.key(), new ArrayList<>());
        ids.remove(vp.id());
        if (ids.isEmpty())
            propertyKeys.remove(vp.key());
        else
            propertyKeys.put(vp.key(), ids);
        long vpCounter = vertexRecord.record().getLong(db.VP_COUNTER);
        if (vpCounter > 0)
            vpCounter--;
        if (vpCounter == db.ID_CACHE_SIZE - 1) // if id set size within cache size, restore the cache
            propertyKeys = readVertexPropertiesByScan(vertex).entrySet().stream().map(entry -> {
                return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().stream().map(Element::id));
            }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, it -> (List<Object>) it.getValue()));

        final Bin vpCounterBin = new Bin(db.VP_COUNTER, Value.get(vpCounter));
        final Bin vertexPropertyIds = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertex.id, vertexPropertyIds, vpCounterBin);
    }

    /**
     * Remove a VertexProperty Record from the database
     *
     * @param property VertexProperty to remove
     */
    @Override
    public void removeVertexProperty(final FireflyVertexProperty property) {
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_PROPERTY_AERO_SET, FireflyId.fromElement(property));
        final Vertex parent = property.element();
        removeIdFromVertexPropertyList((FireflyVertex) parent, property);
        db.delete(key);
    }

    /**
     * Determine if a vertexProperty exists
     *
     * @param vpId VertexProperty id to check
     * @return Boolean vertex property exists
     */
    @Override
    public boolean vertexPropertyExists(final FireflyId vpId) {
        LOG.debug("Checking if vertex property {} exists.", vpId.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, vpId.toNumericId());
        return db.exists(key);
    }


}
