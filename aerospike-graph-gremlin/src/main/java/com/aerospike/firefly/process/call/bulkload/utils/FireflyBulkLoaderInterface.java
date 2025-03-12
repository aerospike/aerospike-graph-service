package com.aerospike.firefly.process.call.bulkload.utils;

import java.util.Map;

public interface FireflyBulkLoaderInterface {
    void load(String[] args);
    Map<String, Object> getStatus();
}
