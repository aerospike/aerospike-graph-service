package com.aerospike.firefly.structure.id;

import com.aerospike.client.util.Crypto;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdPoly extends FireflyId {
    private final Object id;

    public final Source source;


    // Can be null.
    private final Class userClass;

    protected static Map<Class, GetUserId> CONVERT_TO_USER_CLASS = Map.of(
            Long.class, new GetLongId(),
            Integer.class, new GetIntegerId(),
            Double.class, new GetDoubleId(),
            String.class, new GetStringId()
    );

    protected static Map<Class, Long> CONVERT_TO_STORAGE_IDX = Map.of(
            Long.class, 1L,
            Integer.class, 2L,
            Double.class, 3L,
            String.class, 5L
    );
    private final byte[] hash;

    /**
     * Constructor for Numeric Firefly Id. Object class assumed.
     *
     * @param id Id to construct with.
     */
    // Package private. Only the factory should be instantiating this.
    private FireflyIdPoly(final Object id, final String setName) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null.");
        }
        if (String.class.isAssignableFrom(id.getClass())) {
            this.source = Source.STRING;
            this.userClass = String.class;
            this.id = id;
        } else if (Long.class.isAssignableFrom(id.getClass())) {
            this.source = Source.NUMBER;
            this.userClass = Long.class;
            this.id = id;
        } else if (Integer.class.isAssignableFrom(id.getClass())) {
            this.source = Source.NUMBER;
            this.userClass = Integer.class;
            this.id = ((Integer) id).longValue();
        } else if (Double.class.isAssignableFrom(id.getClass())) {
            this.source = Source.NUMBER;
            this.id = ((Double) id).longValue();
            this.userClass = Double.class;

        } else {
            throw new IllegalArgumentException("Id must be a String or Number.");
        }
        this.hash = getIdHash(id, setName);
    }

    private FireflyIdPoly(final Object id, final Class userClass, final String setName) {
        this.userClass = userClass;
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null.");
        } else if (Long.class.isAssignableFrom(id.getClass())) {
            this.source = Source.NUMBER;
            this.id = id;
        } else if (Integer.class.isAssignableFrom(id.getClass())) {
            this.source = Source.NUMBER;
            this.id = ((Integer) id).longValue();
        } else if (Double.class.isAssignableFrom(id.getClass())) {
            this.source = Source.NUMBER;
            this.id = ((Double) id).longValue();

        } else {
            throw new IllegalArgumentException("Id must be a String or Number.");
        }
        this.hash = getIdHash(id, setName);
        if (!CONVERT_TO_STORAGE_IDX.containsKey(this.userClass)) {
            // Should not happen in production, but add case for it anyway.
            throw new RuntimeException(String.format("Error, cannot create numeric id with user class of %s.", this.userClass.getName()));
        }
    }

    private FireflyIdPoly(final byte[] hash, final String setName) {
        this.source = Source.HASH;
        this.hash = hash;
        this.id = null;
        this.userClass = null;
    }

    /**
     * Create a FireflyId from a user supplied id value (type checked only at runtime)
     * @param id The user supplied id
     * @param setName the name of the Aerospike set this id belongs to
     * @return a FireflyId
     */
    public static FireflyIdPoly fromObject(final Object id, final String setName) {
        return new FireflyIdPoly(id, setName);
    }

    /**
     * Create a FireflyId from a user supplied id value (type checked only at runtime)
     * @param id The user supplied id
     * @param userClass The class of the user supplied id
     * @param setName the name of the Aerospike set this id belongs to
     * @return a FireflyId
     */
    public static FireflyIdPoly fromObject(final Object id, final Class userClass, final String setName) {
        return new FireflyIdPoly(id, userClass, setName);
    }

    /**
     * Create a FireflyId from an Aerospike Key digest
     * @param bytes The digest bytes
     * @param setName the name of the Aerospike Set this id belongs to
     * @return a FireflyId
     */
    public static FireflyIdPoly fromHash(final byte[] bytes, final String setName) {
        return new FireflyIdPoly(bytes, setName);
    }

    /**
     * Return the origional value of the id supplied by the user.
     * @return the origional value of the id supplied by the user.
     */
    @Override
    public Object getUserId() {
        if (this.userClass == null) throw new RuntimeException("Error, cannot get user id for hash id.");
        if (CONVERT_TO_USER_CLASS.containsKey(userClass)) {
            return CONVERT_TO_USER_CLASS.get(userClass).getUserId(id);
        } else {
            throw new RuntimeException(String.format("Error, cannot convert numeric id to user class of %s.", userClass.getName()));
        }
    }

    /**
     * Return the user supplied id value in the format it is stored within Aerospike
     * @return the user supplied id value in the format it is stored within Aerospike
     */
    @Override
    public Object getStorageId() {
        return id;
    }

    @Override
    public Long getStorageTypeIdx() {
        return CONVERT_TO_STORAGE_IDX.get(userClass);
    }

    @Override
    public Object getCachedId() {
        return getStorageId();
    }

    @Override
    public byte[] getKeyHash() {
        return hash;
    }


    @Override
    public String toString() {
        return Crypto.encodeBase64(hash);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof FireflyId) return Arrays.equals(getKeyHash(), ((FireflyId) o).getKeyHash());
        return false;
    }

    static abstract class GetUserId {
        public abstract Object getUserId(final Object id);
    }

    static class GetLongId extends GetUserId {
        @Override
        public Object getUserId(final Object id) {
            return ((Number) id).longValue();
        }
    }

    static class GetIntegerId extends GetUserId {
        @Override
        public Object getUserId(final Object id) {
            return ((Number) id).intValue();
        }
    }

    static class GetDoubleId extends GetUserId {
        @Override
        public Object getUserId(final Object id) {
            return ((Number) id).doubleValue();
        }
    }

    static class GetStringId extends GetUserId {
        @Override
        public Object getUserId(final Object id) {
            return id.toString();
        }
    }
}
