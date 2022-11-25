package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.structure.id.FireflyId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
        final long idtypidx = record.getLong(this.ac.ID_TYPE);
        return idStorageTypeToOriginalType(idval, idtypidx);
    }

    public Record record() {
        return record;
    }

    // Construct an Aerospike key from a Firefly ID
    public static Key getKey(final String namespace, final String set, final FireflyId id) {
        return new Key(namespace, set, Value.get(id.getStorageId()));
    }

    public static FireflyRecord read(final AerospikeConnection db, final String set, final FireflyId id) {
        final Key key = getKey(db.getNamespace(), set, id);
        final Record record = db.read(key);
        if (record == null)
            return null;

        return new FireflyRecord(db, key, record);
    }

    public static List<FireflyRecord> batchRead(final AerospikeConnection db, final String set, final List<FireflyId> ids) {
        if (ids.size() == 0) {
            return new ArrayList<>();
        }

        // Batch reading in Aerospike is capped based on settings in the server.
        final Map<FireflyId, FireflyRecord> idToRecord = new HashMap<>();
        final Set<FireflyId> uniqueIds = new HashSet<>(ids);

        // Batch reading in Aerospike is capped based on settings in the server.
        for (int i = 0; i < uniqueIds.size(); i = Math.min(i + db.AEROSPIKE_BATCH_READ_SIZE, uniqueIds.size())) {
            final List<FireflyId> subList = uniqueIds.stream().skip(i).
                    limit(Math.min(uniqueIds.size(), i + db.AEROSPIKE_BATCH_READ_SIZE)).collect(Collectors.toList());
            executeBatchRead(db, set, idToRecord, subList);
        }
        return ids.stream().filter(idToRecord::containsKey).map(idToRecord::get).collect(Collectors.toList());
    }

    private static void executeBatchRead(final AerospikeConnection db,
                                         final String set,
                                         final Map<FireflyId, FireflyRecord> idToRecord,
                                         final List<FireflyId> idsToRead) {
        final Record[] records = db.read(idsToRead.stream().map(idd ->
                getKey(db.getNamespace(), set, idd)).distinct().toArray(Key[]::new));
        for (int i = 0; i < records.length; i++) {
            if (records[i] != null) {
                final FireflyId idd = idsToRead.get(i);
                final FireflyRecord fireflyRecord = new FireflyRecord(db, getKey(db.getNamespace(), set, idd), records[i]);
                idToRecord.put(idd, fireflyRecord);
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
    protected static void write(final AerospikeConnection db,
                                final String set,
                                final FireflyId id,
                                final int generation,
                                final Bin... bins) {
        final Key key = getKey(db.getNamespace(), set, id);
        final Bin idTypeBin = new Bin(db.ID_TYPE, Value.get(id.getStorageTypeIdx()));
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
        final Key key = getKey(db.getNamespace(), set, id);
        final List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
        if (generation == -1) {
            final Bin idTypeBin = new Bin(db.ID_TYPE, Value.get(id.getStorageTypeIdx()));
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
}
