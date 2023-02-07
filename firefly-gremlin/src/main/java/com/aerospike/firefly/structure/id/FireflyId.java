package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;

import java.util.Arrays;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyId implements Comparable {
    public enum Source {
        NUMBER, STRING, HASH, COMPOSITE
    }

    public abstract Object getUserId();

    public abstract Object getStorageId();

    public abstract Long getStorageTypeHint();

    public abstract boolean equals(Object o);

    public abstract Object getCachedId();

    public abstract byte[] getKeyHash();

    public abstract String getKeyHashBase64();


    @Override
    public int hashCode() {
       return this.getStorageId() == null ? Arrays.hashCode(this.getKeyHash()) : this.getStorageId().hashCode();
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof FireflyId) {
            final byte[] thisStorageId = getKeyHash();
            final byte[] otherStorageId = ((FireflyId) o).getKeyHash();
            return Arrays.compare(thisStorageId, otherStorageId);
        } else {
            throw new IllegalArgumentException("Cannot compare FireflyId to " + o.getClass().getName());
        }
    }

    /**
     * Uses the Aerospike Client Crypto routines to produce a RIPEMD160 hash of the string capable of retrieving the record by digest.
     *
     * @param id      the user provided string id
     * @param setName the Aerospike namespace
     * @return the digest of the string
     */
    public static byte[] getIdHash(Object id, String setName) {
        final Value keyValue = Value.get(id);
        byte[] hash = Crypto.computeDigest(setName, keyValue);
        return hash;
    }
}
