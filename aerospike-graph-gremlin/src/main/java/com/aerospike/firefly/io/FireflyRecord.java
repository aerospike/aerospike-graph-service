package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyRecord {
    //Map from classes we support as keys to integer type hint values
    private static final Map<Class<? extends Serializable>, Long> SupportedKeyTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(String.class, 5L);
    }};
    //Map from classes we support as TinkerPop ids to on disk type hints. NOTE: String not fully supported yet
    private static final Map<Class<? extends Serializable>, Long> SupportedIdTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(String.class, 5L);
    }};
    private final Key key;
    private final Record record;
    private final AerospikeConnection ac;

    /**
     * Wrapper for AerospikeRecord
     *
     * @param ac     AerospikeConnection instance
     * @param key    Aerospike Key to wrap
     * @param record Aerospike Record to wrap
     */
    private FireflyRecord(final AerospikeConnection ac,
                          final Key key,
                          final Record record) {
        this.ac = ac;
        this.key = key;
        this.record = record;
    }

    public Key key() {
        return this.key;
    }

    //cast an ID read from disk back to its original type when it was provided by the user
    private static Object idStorageTypeToOriginalType(final Object storedId, final Class<? extends Serializable> origType) {
        if (origType.equals(Long.class))
            return storedId;
        if (origType.equals(Integer.class))
            return Math.toIntExact((Long) storedId);
        if (origType.equals(String.class))
            return storedId.toString();
        throw new UnsupportedOperationException(storedId.getClass() + " is not a supported id type");
    }


    //cast an ID read from disk back to its original type when it was provided by the user
    public static Object idStorageTypeToOriginalType(final Object storedId, final long originalTypeHint) {
        return FireflyRecord.idStorageTypeToOriginalType(storedId, idTypeFromHint(originalTypeHint));
    }


    //Convert a numeric type-hint stored on disk to the class it represents
    private static Class<? extends Serializable> idTypeFromHint(final long hint) {
        return SupportedIdTypes.entrySet().stream().filter(e -> e.getValue() == hint).collect(Collectors.toList()).get(0).getKey();
    }

    public Record record() {
        return record;
    }

    public static Key getKey(final AerospikeConnection db, final String set, final FireflyId id) {
        if (id.getStorageId() != null)
            return new Key(db.getNamespace(), set, Value.get(id.getStorageId()));
        else
            return new Key(db.getNamespace(), id.getKeyHash(), set, Value.NULL);
    }

    public static Key getMergeEdgeKey(final FireflyGraph graph, final Object fromV, final Object toV) {
        final FireflyId fromVId = graph.getIdFactory().createVertexId(fromV);
        final FireflyId toVId = graph.getIdFactory().createVertexId(toV);
        final byte[] compoundKeyHash = ArrayUtils.addAll(fromVId.getKeyHash(), toVId.getKeyHash());
        final AerospikeConnection db = graph.getBaseGraph();
        return new Key(db.getNamespace(), db.GRAPH_METADATA_SET, Value.get(compoundKeyHash));
    }

    public static FireflyRecord read(final AerospikeConnection db, final String set, final FireflyId id) {
        final Key key = getKey(db, set, id);
        final Policy policy = new Policy();
        final Record record = db.read(key, policy, db.transactionCache.get());
        if (record == null)
            return null;
        return new FireflyRecord(db, key, record);
    }

    public static List<FireflyRecord> batchRead(final AerospikeConnection db, final ReadInfo readInfo) {
        // Check if empty and return empty if it is.
        if (readInfo.ids.isEmpty()) {
            return new ArrayList<>();
        }

        // Batch reading in Aerospike is capped based on settings in the server.
        final Map<FireflyId, FireflyRecord> idToRecord = new HashMap<>();
        final Set<FireflyId> uniqueIds = new HashSet<>(readInfo.ids);

        // Batch reading in Aerospike is capped based on settings in the server.
        for (int i = 0; i < uniqueIds.size(); i += db.AEROSPIKE_BATCH_READ_SIZE) {
            // Generate sub list using current index and batch size.
            final List<FireflyId> subList = uniqueIds.stream().skip(i).limit(db.AEROSPIKE_BATCH_READ_SIZE).collect(Collectors.toList());

            // Execute batch read. subList ids are read from the database.
            executeBatchRead(db, readInfo, idToRecord, subList);
        }

        // Return the records in the same order as the ids, removing any null items.
        return readInfo.ids.stream().filter(idToRecord::containsKey).map(idToRecord::get).collect(Collectors.toList());
    }

    private static void executeBatchRead(final AerospikeConnection db,
                                         final ReadInfo readInfo,
                                         final Map<FireflyId, FireflyRecord> idToRecord,
                                         final List<FireflyId> idsToRead) {
        // Read all records from the database.
        // Before reading id list must be converted to array of keys.
        final List<Key> keyList = idsToRead.stream().map(id -> getKey(db, readInfo.set, id)).collect(Collectors.toList());

        final Record[] records;
        if (readInfo.requiredProperties == null) {
            // All property reads uses transaction cache.
            records = db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), db.transactionCache.get());
        } else {
            final List<Operation> operations = new ArrayList<>();
            db.vertexNonPropertyBins.forEach(bin -> operations.add(Operation.get(bin)));
            if (!readInfo.requiredProperties.isEmpty()) {
                // No cache for non-empty required properties.
                final List<Value> properties = readInfo.requiredProperties.stream().map(Value::get).collect(Collectors.toList());
                db.vertexPropertyBins.forEach(bin -> operations.add(MapOperation.getByKeyList(bin, properties, MapReturnType.UNORDERED_MAP)));
                records = db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), null, operations.toArray(Operation[]::new));
            } else {
                // No property read uses empty property transaction cache.
                records = db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), db.emptyPropsTransactionCache.get(), operations.toArray(Operation[]::new));
            }
        }
        for (int i = 0; i < records.length; i++) {
            if (records[i] != null) {
                // Add id/record pair to the map.
                final FireflyId id = idsToRead.get(i);
                final FireflyRecord fireflyRecord = new FireflyRecord(db, getKey(db, readInfo.set, id), records[i]);
                idToRecord.put(id, fireflyRecord);
            }
        }
    }

    /**
     * Batch read edges given a list of their IDs. Returns a list of FireflyRecord of phat edges which contain the
     * individual edges' data.
     * @param db    AerospikeConnection instance
     * @param ids   IDs of edges requested
     * @return  Map of Edge FireflyIds to the FireflyEdgeRecord of a phat edge containing that Edge's data
     */
    public static Map<FireflyId, FireflyEdgeRecord> batchReadPhatEdges(final AerospikeConnection db,
                                                                       final List<FireflyId> ids) {

        // Requested IDs to their respective FireflyRecord.
        final Map<FireflyId, FireflyEdgeRecord> records = new HashMap<>();
        // Check if empty and return empty if it is.
        if (ids.isEmpty()) {
            return records;
        }

        // Map of phat Edge IDs (long) to their respective Key to keep track of whether we've added it.
        final Map<Long, Key> edgeStorageIdToKey = new HashMap<>();

        // Deduplicate the ids.
        final Set<FireflyId> uniqueIds = new HashSet<>(ids);
        // Deduplicate the phat edge ids.
        for (final FireflyId edgeId : uniqueIds) {
            if (!edgeStorageIdToKey.containsKey(edgeId.getStorageId())) {
                edgeStorageIdToKey.put((long) edgeId.getStorageId(), getKey(db, db.EDGE_AERO_SET, edgeId));
            }
        }
        final Collection<Key> keys = edgeStorageIdToKey.values();

        // Map of phat Edge IDs to their respective FireflyRecord.
        final Map<Long, FireflyEdgeRecord> phatEdgeStorageIdToRecord = new HashMap<>();

        for (int i = 0; i < keys.size(); i += db.AEROSPIKE_BATCH_READ_SIZE) {
            // Generate sub list using current index and batch size.
            final List<Key> subKeys = keys.stream().skip(i).limit(db.AEROSPIKE_BATCH_READ_SIZE).collect(Collectors.toList());

            // Execute batch read. subList ids are read from the database.
            executeBatchReadPhatEdges(db, phatEdgeStorageIdToRecord, subKeys);
        }

        for (final FireflyId id : ids) {
            records.put(id, phatEdgeStorageIdToRecord.get(id.getStorageId()));
        }
        return records;
    }

    private static void executeBatchReadPhatEdges(final AerospikeConnection db,
                                                  final Map<Long, FireflyEdgeRecord> phatEdgeStorageIdToRecord,
                                                  final List<Key> keysToRead) {
        final Record[] records = db.dynamicBatchRead(keysToRead.toArray(Key[]::new), null, db.transactionCache.get());
        for (int i = 0; i < records.length; i++) {
            if (records[i] != null) {
                // Add storage id to record pair to the map.
                final Key key = keysToRead.get(i);
                final Long storageId = key.userKey.toLong();
                final FireflyEdgeRecord fireflyRecord = new FireflyEdgeRecord(records[i], db);
                phatEdgeStorageIdToRecord.put(storageId, fireflyRecord);
            }
        }
    }

    /**
     * Construct a FireflyRecord from an Aerospike KeyRecord
     *
     * @param db            AerospikeConnection instance
     * @param keyRecord     Aerospike KeyRecord
     * @return FireflyRecord
     */
    public static FireflyRecord fromRecord(final AerospikeConnection db, final KeyRecord keyRecord) {
        if (keyRecord.record == null)
            return null;

        return new FireflyRecord(db, keyRecord.key, keyRecord.record);
    }

    @Override
    public int hashCode() {
        return record.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o.getClass().equals(this.getClass()) &&
                ((FireflyRecord) o).key.equals(key) &&
                ((FireflyRecord) o).record.equals(record);
    }

    @Override
    public String toString() {
        return key.toString();
    }
}
