package com.aerospike.firefly.structure.id;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyId {
    public abstract Object getUserId();
    public abstract Object getStorageId();
    public abstract Long getStorageTypeIdx();
    public abstract boolean equals(Object o);
    public abstract Object getCachedId();

    @Override
    public int hashCode() {
        return getStorageId().hashCode();
    }
}
