package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;

import java.util.Arrays;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyId implements Comparable {
    public enum Source {
        NUMBER, STRING, HASH, COMPOSITE, BYTE_ARRAY
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
        return this.getKeyHash() == null ? this.getStorageId().hashCode() : Arrays.hashCode(this.getKeyHash());
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
}
