package com.aerospike.firefly.structure.id;

import java.util.Arrays;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public interface FireflyId extends Comparable {

    Object getUserId();

    Object getStorageId();

    Long getStorageTypeHint();

    boolean equals(Object o);

    Object getCachedId();

    byte[] getKeyHash();

    String getKeyHashBase64();

    String getKeyHashString();

    default int compareTo(final Object o) {
        if (o instanceof FireflyId) {
            final byte[] thisStorageId = getKeyHash();
            final byte[] otherStorageId = ((FireflyId) o).getKeyHash();
            return Arrays.compare(thisStorageId, otherStorageId);
        } else {
            throw new IllegalArgumentException("Cannot compare FireflyId to " + o.getClass().getName());
        }
    }
}
