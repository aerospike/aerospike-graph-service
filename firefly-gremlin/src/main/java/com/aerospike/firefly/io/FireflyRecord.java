package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.structure.id.FireflyId;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyRecord {
    private static final Map<Class<? extends Serializable>, Long> SupportedKeyTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(String.class, 5L);
    }};
    private static final Map<Class<? extends Serializable>, Long> SupportedIdTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(String.class, 5L);
    }};
    protected final Key key;
    public final Record record;
    public final Class<? extends Serializable> userClass;
    public final Class<? extends Serializable> storageClass;
    private static final WritePolicy sendKeyWritePolicy = new WritePolicy();

    static {
        sendKeyWritePolicy.sendKey = true;
    }

    private final AerospikeConnection ac;


    private FireflyRecord(AerospikeConnection ac,
                          final Key key,
                          final Record record,
                          final Class<? extends Serializable> userClass,
                          final Class<? extends Serializable> storageClass) {
        this.ac = ac;
        this.key = key;
        this.record = record;
        this.userClass = userClass;
        this.storageClass = storageClass;
    }

    private static Object idStorageTypeToOriginalType(final Object storedId, final Class<? extends Serializable> origType) {
        if (origType.equals(Long.class))
            return storedId;
        if (origType.equals(Integer.class))
            return Math.toIntExact((Long) storedId);
        if (origType.equals(String.class))
            return storedId.toString();
        throw new UnsupportedOperationException(storedId.getClass() + " is not a supported id type");
    }

    public Key key(){
        return this.key;
    }
    public static Object idStorageTypeToOriginalType(final Object storedId, final long originalTypeIdx) {
        return FireflyRecord.idStorageTypeToOriginalType(storedId, idTypeFromIdx(originalTypeIdx));
    }

    private static Object keyToStorageType(final Object origId) {
        if (Integer.class.equals(origId.getClass()))
            return ((Integer) origId).longValue();
        return origId;
    }

    private static Class<? extends Serializable> idTypeFromIdx(final long idx) {
        return SupportedIdTypes.entrySet().stream().filter(e -> e.getValue() == idx).collect(Collectors.toList()).get(0).getKey();
    }

    private static Long getSupportedIdTypeIdx(final Class clazz) {
        if (!AerospikeConnection.IdToDiskTypeMap.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported id type");
        return SupportedKeyTypes.get(clazz);
    }

    private static Long getSupportedKeyTypeIdx(final Class clazz) {
        if (!AerospikeConnection.KeyToDiskTypeMap.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported id type");
        return AerospikeConnection.SupportedValueTypes.get(clazz);
    }


    public Object id() {
        final long idval = key.userKey.toLong();
        final long idtypidx = record.getLong(this.ac.ID_TYPE);
        return idStorageTypeToOriginalType(idval, idtypidx);
    }
    public Record record(){
        return record;
    }


    public static Key getKey(final String namespace, final String set, final FireflyId id) {
        final Key key;
        if (id.value().getClass().equals(Long.class))
            key = new Key(namespace, set, (Long) id.value());
        else if (id.value().getClass().equals(Integer.class))
            key = new Key(namespace, set, (Long) keyToStorageType(id.value()));
        else if (id.value().getClass().equals(String.class))
            key = new Key(namespace, set, (String) id.value());
        else if (id.value().getClass().equals(byte[].class))
            key = new Key(namespace, set, (byte[]) id.value());
        else
            throw new UnsupportedOperationException(id.value().getClass() + " unsuppored key type");
        return key;
    }

    private static Key getElementKey(final String namespace, final String set, final FireflyId id) {
        final Key key;
        if (id.value().getClass().equals(Long.class))
            key = new Key(namespace, set, (Long) id.value());
        else if (id.value().getClass().equals(Integer.class))
            key = new Key(namespace, set, (Long) keyToStorageType(id.value()));
        else if (id.value().getClass().equals(String.class))
            key = new Key(namespace, set, Long.parseLong((String) id.value()));
        else if (id.value().getClass().equals(byte[].class))
            key = new Key(namespace, set, (byte[]) id.value());
        else
            throw new UnsupportedOperationException(id.value().getClass() + " unsuppored key type");
        return key;
    }

    public static FireflyRecord read(final AerospikeConnection db, final String set, final FireflyId id) {
        final Key key = getKey(db.namespace, set, id);
        final Record record = db.read(key);
        if (record == null)
            return null;
        final long idTypeIdx = record.getLong(db.ID_TYPE);
        final Class<? extends Serializable> userClass = idTypeFromIdx(idTypeIdx);
        final Class<? extends Serializable> storageClass = AerospikeConnection.KeyToDiskTypeMap.get(userClass);

        return new FireflyRecord(db, key, record, userClass, storageClass);
    }
    protected static FireflyRecord fromRecord(final AerospikeConnection db, final Key key, final Record record) {
        if (record == null)
            return null;
        final long idTypeIdx = record.getLong(db.ID_TYPE);
        final Class<? extends Serializable> userClass = idTypeFromIdx(idTypeIdx);
        final Class<? extends Serializable> storageClass = AerospikeConnection.KeyToDiskTypeMap.get(userClass);

        return new FireflyRecord(db, key, record, userClass, storageClass);
    }

    protected static void write(final AerospikeConnection db,
                                final String set,
                                final FireflyId id,
                                final Bin... bins) {
        final Long supportedIdTypeIdx = getSupportedKeyTypeIdx(id.value().getClass());
        final Key key = getKey(db.namespace, set, id);
        final Bin idTypeBin = new Bin(db.ID_TYPE, Value.get(supportedIdTypeIdx));
        final List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
        listOfBins.add(idTypeBin);

        try {
            db.client.put(sendKeyWritePolicy, key, listOfBins.toArray(new Bin[0]));
        } catch (com.aerospike.client.AerospikeException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void writeElement(final AerospikeConnection db,
                                       final String set,
                                       final FireflyId id,
                                       final Bin... bins) {
        final Long supportedIdTypeIdx = getSupportedIdTypeIdx(id.value().getClass());
        final Key key = getElementKey(db.namespace, set, id);
        final Bin idTypeBin = new Bin(db.ID_TYPE, Value.get(supportedIdTypeIdx));
        final List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
        listOfBins.add(idTypeBin);
        try {
            db.client.put(sendKeyWritePolicy, key, listOfBins.toArray(new Bin[0]));
        } catch (com.aerospike.client.AerospikeException e) {
            throw new RuntimeException(e);
        }
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
