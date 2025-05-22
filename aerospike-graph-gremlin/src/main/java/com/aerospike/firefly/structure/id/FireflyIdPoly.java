package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static com.aerospike.firefly.structure.id.FireflyIdFactory.VERTEX_ID_TYPE_TO_HINT;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdPoly implements FireflyId {
    private static Map<Class, GetUserId> CONVERT_TO_USER_CLASS = Map.of(
            Long.class, new GetLongId(),
            Integer.class, new GetIntegerId(),
            Double.class, new GetDoubleId(),
            String.class, new GetStringId()
    );

    protected Object id;
    protected Object userId = null;
    // Can be null.
    private final Class userClass;
    private final String setName;
    // Lazily instantiate this.
    private byte[] hash = null;
    private String hashString = null;
    private String toString = null;

    /**
     * Constructor for Numeric Firefly Id. Object class assumed.
     *
     * @param id Id to construct with.
     */
    protected FireflyIdPoly(final Object id, final String setName) {
        this(id, id.getClass(), setName);
    }

    private FireflyIdPoly(final Object id, final Class<?> userClass, final String setName) {
        this.userClass = userClass;
        this.setName = setName;
        initialize(id);
    }

    protected void initialize(final Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null.");
        } else if (Number.class.isAssignableFrom(id.getClass())) {
            this.id = ((Number) id).longValue();
        } else if (String.class.isAssignableFrom(id.getClass())) {
            this.id = id;
        } else {
            throw new IllegalArgumentException("Id must be a String or Number. Id provided was '" + id.getClass() + "'.");
        }
        if (!VERTEX_ID_TYPE_TO_HINT.containsKey(this.userClass) &&
                !VERTEX_ID_TYPE_TO_HINT.containsKey(this.userClass.getSuperclass())) {
            // Should not happen in production, but add case for it anyway.
            throw new RuntimeException(String.format("Error, cannot create poly id with user class of %s.",
                    this.userClass.getName()));
        }
    }

    private FireflyIdPoly(final byte[] hash, final String setName) {
        this.hash = hash;
        this.id = null;
        this.userClass = null;
        this.setName = setName;
    }

    /**
     * Create a FireflyId from a user supplied id value (type checked only at runtime)
     *
     * @param id      The user supplied id
     * @param setName the name of the Aerospike set this id belongs to
     * @return a FireflyId
     */
    static FireflyIdPoly fromObject(final Object id, final String setName) {
        return new FireflyIdPoly(id, setName);
    }

    /**
     * Create a FireflyId from a user supplied id value (type checked only at runtime)
     *
     * @param id        The user supplied id
     * @param userClass The class of the user supplied id
     * @param setName   the name of the Aerospike set this id belongs to
     * @return a FireflyId
     */
    static FireflyIdPoly fromObject(final Object id, final Class userClass, final String setName) {
        return new FireflyIdPoly(id, userClass, setName);
    }

    /**
     * Create a FireflyId from an Aerospike Key digest
     *
     * @param bytes   The digest bytes
     * @param setName the name of the Aerospike Set this id belongs to
     * @return a FireflyId
     */
    static FireflyIdPoly fromHash(final byte[] bytes, final String setName) {
        return new FireflyIdPoly(bytes, setName);
    }

    /**
     * Create a FireflyId from an Aerospike Key digest as a String
     *
     * @param hash    The digest bytes in String form
     * @param setName the name of the Aerospike Set this id belongs to
     * @return a FireflyId
     */
    static FireflyIdPoly fromHashString(final String hash, final String setName) {
        return new FireflyIdPoly(hash.getBytes(StandardCharsets.ISO_8859_1), setName);
    }

    /**
     * Create a FireflyId from an Aerospike Key digest
     *
     * @param base64hash The base64 encoded hash string
     * @param setName    the name of the Aerospike Set this id belongs to
     * @return a FireflyId
     */
    static FireflyIdPoly fromBase64Hash(final String base64hash, final String setName) {
        return new FireflyIdPoly(Crypto.decodeBase64(base64hash.getBytes(), 0, base64hash.getBytes().length), setName);
    }

    /**
     * Return the original value of the id supplied by the user.
     *
     * @return the original value of the id supplied by the user.
     */
    @Override
    public Object getUserId() {
        if (this.userId != null) {
            return this.userId;
        }
        if (this.userClass == null) {
            throw new RuntimeException("Error, cannot get user id for hash id.");
        }
        if (CONVERT_TO_USER_CLASS.containsKey(userClass)) {
            this.userId = CONVERT_TO_USER_CLASS.get(userClass).getUserId(id);
        } else if (CONVERT_TO_USER_CLASS.containsKey(userClass.getSuperclass())) {
            this.userId = CONVERT_TO_USER_CLASS.get(userClass.getSuperclass()).getUserId(id);
        } else {
            throw new RuntimeException(String.format("Error, cannot convert numeric id to user class of %s.", userClass.getName()));
        }
        return this.userId;
    }

    /**
     * Return the user supplied id value in the format it is stored within Aerospike
     *
     * @return the user supplied id value in the format it is stored within Aerospike
     */
    @Override
    public Object getStorageId() {
        return id;
    }

    @Override
    public Long getStorageTypeHint() {
        if (VERTEX_ID_TYPE_TO_HINT.containsKey(userClass)) {
            return VERTEX_ID_TYPE_TO_HINT.get(userClass);
        } else {
            return VERTEX_ID_TYPE_TO_HINT.get(userClass.getSuperclass());
        }
    }

    @Override
    public Object getCachedId() {
        return getStorageId();
    }

    @Override
    public byte[] getKeyHash() {
        if (this.hash == null) {
            this.hash = getIdHash(this.setName);
        }
        return this.hash;
    }

    @Override
    public String getKeyHashString() {
        if (this.hashString == null) {
            this.hashString = new String(getKeyHash(), StandardCharsets.ISO_8859_1);
        }
        return this.hashString;
    }

    @Override
    public String getKeyHashBase64() {
        return Crypto.encodeBase64(getKeyHash());
    }

    /**
     * Uses the Aerospike Client Crypto routines to produce a RIPEMD160 hash of the string capable of retrieving the record by digest.
     *
     * @param setName the Aerospike namespace
     * @return the digest of the string
     */
    protected byte[] getIdHash(String setName) {
        final Value keyValue = Value.get(this.id);
        return Crypto.computeDigest(setName, keyValue);
    }

    @Override
    public String toString() {
        if (this.hash == null) {
            this.hash = getIdHash(this.setName);
        }
        if (this.toString == null) {
            this.toString = Crypto.encodeBase64(hash);
        }
        return toString;
    }

    @Override
    public int hashCode() {
        return this.getKeyHash() == null ? this.getStorageId().hashCode() : Arrays.hashCode(this.getKeyHash());
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
