package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.process.call.FireflyBulkLoaderServiceFactory;

import java.util.Map;


public class BulkLoaderCallEntryPoint implements FireflyBulkLoaderServiceFactory.BulkLoad {
    @Override
    public void perform(final Map params) {
        SparkBulkLoaderMain.main(new String[0]);
    }
}
