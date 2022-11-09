package com.aerospike.firefly.structure.id;

public interface FireflyId {
    Object getUserId();
    Object getStorageId();
    Long getStorageTypeIdx();
}
