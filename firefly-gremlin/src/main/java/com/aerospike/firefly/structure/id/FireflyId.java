package com.aerospike.firefly.structure.id;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyId implements Comparable {
    public abstract Object getUserId();
    public abstract Object getStorageId();
    public abstract Long getStorageTypeIdx();
    public abstract boolean equals(Object o);
    public abstract Object getCachedId();

    @Override
    public int hashCode() {
        return getStorageId().hashCode();
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof FireflyId) {
            final Object thisStorageId = getStorageId();
            final Object otherStorageId = ((FireflyId) o).getStorageId();
            if (thisStorageId instanceof Comparable && otherStorageId instanceof Comparable) {
                return ((Comparable) thisStorageId).compareTo(otherStorageId);
            } else {
                throw new IllegalArgumentException("Storage IDs must be comparable of comparable types.");
            }
        } else {
            throw new IllegalArgumentException("Cannot compare FireflyId to " + o.getClass().getName());
        }
    }
}
