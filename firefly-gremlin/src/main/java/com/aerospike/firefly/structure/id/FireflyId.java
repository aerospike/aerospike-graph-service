package com.aerospike.firefly.structure.id;

public abstract class FireflyId {
    public abstract Object getUserId();
    public abstract Object getStorageId();
    public abstract Long getStorageTypeIdx();
    public abstract boolean equals(Object o);
    public abstract Object getCachedId();
}
