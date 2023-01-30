package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyIdPoly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    protected final Key key;
    public final Record record;
    private static final WritePolicy sendKeyWritePolicy = new WritePolicy();

    static {
        sendKeyWritePolicy.sendKey = true;
    }

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
    public static Object idStorageTypeToOriginalType(final Object storedId, final long originalTypeIdx) {
        return FireflyRecord.idStorageTypeToOriginalType(storedId, idTypeFromIdx(originalTypeIdx));
    }


    //Convert a numeric type-hint stored on disk to the class it represents
    private static Class<? extends Serializable> idTypeFromIdx(final long idx) {
        return SupportedIdTypes.entrySet().stream().filter(e -> e.getValue() == idx).collect(Collectors.toList()).get(0).getKey();
    }

    //return the TinkerPop ID of this firefly record
    public Object id() {
        final long idval = key.userKey.toLong();
        final long idtypidx = record.getLong(this.ac.IT_TYPE_BIN);
        return idStorageTypeToOriginalType(idval, idtypidx);
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

    public static FireflyRecord read(final AerospikeConnection db, final String set, final FireflyId id) {
        final Key key = getKey(db, set, id);
        final Record record = db.read(key, AerospikeConnection.sendKeyReadPolicy);
        if (record == null)
            return null;
        return new FireflyRecord(db, key, record);
    }

    public static List<FireflyRecord> batchRead(final AerospikeConnection db, final String set, final List<FireflyId> ids) {
        // Check if empty and return empty if it is.
        if (ids.size() == 0) {
            return new ArrayList<>();
        }

        // Batch reading in Aerospike is capped based on settings in the server.
        final Map<FireflyId, FireflyRecord> idToRecord = new HashMap<>();
        final Set<FireflyId> uniqueIds = new HashSet<>(ids);

        // Batch reading in Aerospike is capped based on settings in the server.
        for (int i = 0; i < uniqueIds.size(); i += db.AEROSPIKE_BATCH_READ_SIZE) {
            // Generate sub list using current index and batch size.
            final List<FireflyId> subList = uniqueIds.stream().skip(i).limit(db.AEROSPIKE_BATCH_READ_SIZE).collect(Collectors.toList());

            // Execute batch read. subList ids are read from the database.
            executeBatchRead(db, set, idToRecord, subList);
        }

        // Return the records in the same order as the ids, removing any null items.
        idToRecord.values().removeIf(Objects::isNull);
        return ids.stream().map(id -> {
            Iterator<Map.Entry<FireflyId, FireflyRecord>> i = idToRecord.entrySet().stream().filter(e ->
                    Arrays.equals(e.getKey().getKeyHash(), id.getKeyHash())
            ).iterator();
            if (i.hasNext()) return i.next().getValue();
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private static void executeBatchRead(final AerospikeConnection db,
                                         final String set,
                                         final Map<FireflyId, FireflyRecord> idToRecord,
                                         final List<FireflyId> idsToRead) {
        // Read all records from the database.
        // Before reading id list must be converted to array of keys.
        List<Key> keyList = idsToRead.stream().map(id -> {
            Key key;
            if (id.getClass().equals(FireflyIdComposite.class)) {
                key = new Key(db.getNamespace(), (byte[]) ((FireflyIdComposite) id).getEdgeId().getKeyHash(), set, Value.NULL);
            } else if (((FireflyIdPoly) id).source == FireflyId.Source.HASH) {
                key = getKey(db, set, id);
            } else {
                key = getKey(db, set, id);
            }
            return key;
        }).collect(Collectors.toList());
        Record[] records = db.read(keyList.toArray(Key[]::new));
        for (int i = 0; i < records.length; i++) {
            if (records[i] != null) {
                // Add id/record pair to the map.
                final FireflyId id = idsToRead.get(i);

                final FireflyRecord fireflyRecord = new FireflyRecord(db, getKey(db, set, id), records[i]);
                idToRecord.put(id, fireflyRecord);
            }
        }
    }

    /**
     * Construct a FireflyRecord from an Aerospike Record and Key
     *
     * @param db     AerospikeConnection instance
     * @param key    Aerospike Key
     * @param record Aerospike Record
     * @return FireflyRecord
     */
    public static FireflyRecord fromRecord(final AerospikeConnection db, final Key key, final Record record) {
        if (record == null)
            return null;

        return new FireflyRecord(db, key, record);
    }

    /**
     * Write a new FireflyRecord to disk
     *
     * @param db   AerospikeConnection instance
     * @param set  Aerospike Set to write to
     * @param id   the ID to use
     * @param bins Aerospike data bins
     */
    public static void write(final AerospikeConnection db,
                             final String set,
                             final FireflyId id,
                             final int generation,
                             final Bin... bins) {
        final Key key = getKey(db, set, id);
        final Bin idTypeBin = new Bin(db.IT_TYPE_BIN, Value.get(id.getStorageTypeIdx()));
        final List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
        listOfBins.add(idTypeBin);
        db.write(key, generation, listOfBins.toArray(new Bin[0]));
    }

    /**
     * Write a new FireflyRecord to disk for a TinkerPop Element
     *
     * @param db   AerospikeConnection instance
     * @param set  Aerospike Set to write to
     * @param id   the ID to use
     * @param bins Aerospike data bins
     */
    public static void writeElement(final AerospikeConnection db,
                                    final String set,
                                    final FireflyId id,
                                    final int generation,
                                    final Bin... bins) {
        final Key key = getKey(db, set, id);
        final List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
        if (generation == -1) {
            final Bin idTypeBin = new Bin(db.IT_TYPE_BIN, Value.get(id.getStorageTypeIdx()));
            listOfBins.add(idTypeBin);
        }
        db.write(key, generation, listOfBins.toArray(new Bin[0]));
    }

    @Override
    public int hashCode() {
        return record.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o.getClass().equals(this.getClass()) &&
                ((FireflyRecord) o).key.equals(key) &&
                ((FireflyRecord) o).record.equals(record);
    }

    @Override
    public String toString() {
        return key.toString();
    }

    /**
     * Return the user key associated with this record
     *
     * @return Object user key
     */
    public Object getUserKey() {
        return record.getValue(AerospikeConnection.USER_KEY) == null ? key.userKey.getObject() : record.getValue(AerospikeConnection.USER_KEY);
    }
}
