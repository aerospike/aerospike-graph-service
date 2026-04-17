/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private static final Map<Long, Class<? extends Serializable>> IdHintToType = new HashMap<>();

    static {
        SupportedIdTypes.forEach((clazz, hint) -> IdHintToType.put(hint, clazz));
    }

    private final Key key;
    private final Record record;

    /**
     * Wrapper for AerospikeRecord
     *
     * @param key    Aerospike Key to wrap
     * @param record Aerospike Record to wrap
     */
    private FireflyRecord(final Key key,
                          final Record record) {
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

    private static Class<? extends Serializable> idTypeFromHint(final long hint) {
        final Class<? extends Serializable> type = IdHintToType.get(hint);
        if (type == null) {
            throw new IllegalArgumentException("Unknown id type hint: " + hint);
        }
        return type;
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
        return new Key(db.getNamespace(), db.getConfig().graphMetadataSet, Value.get(compoundKeyHash));
    }

    public static FireflyRecord read(final AerospikeConnection db, final String set, final FireflyId id) {
        final Key key = getKey(db, set, id);
        final Policy policy = new Policy();
        final Record record = db.read(key, policy, db.cacheManager.getTransactionCache());
        if (record == null)
            return null;
        return new FireflyRecord(key, record);
    }

    public static List<FireflyRecord> batchRead(final AerospikeConnection db, final ReadInfo readInfo) {
        if (readInfo.ids.isEmpty()) {
            return new ArrayList<>();
        }

        final Map<FireflyId, FireflyRecord> idToRecord = new HashMap<>();
        final List<FireflyId> uniqueIds = new ArrayList<>(new HashSet<>(readInfo.ids));

        final int batchSize = db.getConfig().aerospikeBatchReadSize;
        for (int i = 0; i < uniqueIds.size(); i += batchSize) {
            executeBatchRead(db, readInfo, idToRecord,
                    uniqueIds.subList(i, Math.min(i + batchSize, uniqueIds.size())));
        }

        final List<FireflyRecord> result = new ArrayList<>(readInfo.ids.size());
        for (final FireflyId id : readInfo.ids) {
            final FireflyRecord record = idToRecord.get(id);
            if (record != null) {
                result.add(record);
            }
        }
        return result;
    }

    private static void executeBatchRead(final AerospikeConnection db,
                                         final ReadInfo readInfo,
                                         final Map<FireflyId, FireflyRecord> idToRecord,
                                         final List<FireflyId> idsToRead) {
        // Read all records from the database.
        // Before reading id list must be converted to array of keys.
        final List<Key> keyList = idsToRead.stream().map(id -> getKey(db, readInfo.set, id)).collect(Collectors.toList());

        final Record[] records;
        if (readInfo.areEdgesRequired && readInfo.requiredProperties == null) {
            // All property and edges reads uses transaction cache.
            records = db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), db.cacheManager.getTransactionCache());
        } else {
            final List<Operation> operations = new ArrayList<>();
            db.getConfig().vertexMiscBins.forEach(bin -> operations.add(Operation.get(bin)));
            if (readInfo.areEdgesRequired) {
                db.getConfig().vertexEdgeBins.forEach(bin -> operations.add(Operation.get(bin)));
            }
            if (readInfo.requiredProperties == null) {
                db.getConfig().vertexPropertyBins.forEach(bin -> operations.add(Operation.get(bin)));
                // read without cache because no edges (see line 152)
                records = db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), null, operations.toArray(Operation[]::new));
            } else if (!readInfo.requiredProperties.isEmpty()) {
                // No cache for non-empty required properties.
                final List<Value> properties = readInfo.requiredProperties.stream().map(propertyKey -> {
                    final Long schemaPropertyKey = db.schemaManager.getVertexPropertyRead(propertyKey);
                    return Value.get(schemaPropertyKey);
                }).collect(Collectors.toList());
                db.getConfig().vertexPropertyBins.forEach(bin -> operations.add(MapOperation.getByKeyList(bin, properties, MapReturnType.UNORDERED_MAP)));
                records = db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), null, operations.toArray(Operation[]::new));
            } else {
                records = readInfo.areEdgesRequired
                        // No property read uses empty property transaction cache when edges are present
                        ? db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), db.cacheManager.getEmptyPropsCache(), operations.toArray(Operation[]::new))
                        // otherwise no cache
                        : db.dynamicBatchRead(readInfo, keyList.toArray(Key[]::new), null, operations.toArray(Operation[]::new));
            }
        }

        for (int i = 0; i < records.length; i++) {
            if (records[i] != null) {
                // Add id/record pair to the map.
                final FireflyId id = idsToRead.get(i);
                final FireflyRecord fireflyRecord = new FireflyRecord(getKey(db, readInfo.set, id), records[i]);
                idToRecord.put(id, fireflyRecord);
            }
        }
    }

    /**
     * Batch read edges given a list of their IDs. Returns a list of FireflyRecord of phat edges which contain the
     * individual edges' data.
     *
     * @param db  AerospikeConnection instance
     * @param ids IDs of edges requested
     * @return Map of Edge FireflyIds to the FireflyEdgeRecord of a phat edge containing that Edge's data
     */
    public static Map<FireflyId, FireflyEdgeRecord> batchReadPhatEdges(final AerospikeConnection db,
                                                                       final List<FireflyId> ids) {
        if (ids.isEmpty()) {
            return new HashMap<>();
        }

        // Deduplicate by storage ID directly — no intermediate HashSet of edge IDs needed.
        final Map<Long, Key> edgeStorageIdToKey = new HashMap<>();
        for (final FireflyId edgeId : ids) {
            edgeStorageIdToKey.putIfAbsent(
                    (long) edgeId.getStorageId(),
                    getKey(db, db.getConfig().edgeAeroSet, edgeId));
        }

        // Use List + subList instead of stream skip/limit (avoids O(n^2) iteration).
        final List<Key> keyList = new ArrayList<>(edgeStorageIdToKey.values());
        final Map<Long, FireflyEdgeRecord> storageIdToRecord = new HashMap<>();
        final int batchSize = db.getConfig().aerospikeBatchReadSize;
        for (int i = 0; i < keyList.size(); i += batchSize) {
            executeBatchReadPhatEdges(db, storageIdToRecord,
                    keyList.subList(i, Math.min(i + batchSize, keyList.size())));
        }

        // Map each requested edge ID to its phat record.
        final Map<FireflyId, FireflyEdgeRecord> records = new HashMap<>(ids.size());
        for (final FireflyId id : ids) {
            records.put(id, storageIdToRecord.get(id.getStorageId()));
        }
        return records;
    }

    private static void executeBatchReadPhatEdges(final AerospikeConnection db,
                                                  final Map<Long, FireflyEdgeRecord> phatEdgeStorageIdToRecord,
                                                  final List<Key> keysToRead) {
        final Record[] records = db.dynamicBatchRead(keysToRead.toArray(Key[]::new), null, db.cacheManager.getTransactionCache());
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
     * @param keyRecord Aerospike KeyRecord
     * @return FireflyRecord
     */
    public static FireflyRecord fromRecord(final KeyRecord keyRecord) {
        if (keyRecord.record == null)
            return null;

        return new FireflyRecord(keyRecord.key, keyRecord.record);
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
