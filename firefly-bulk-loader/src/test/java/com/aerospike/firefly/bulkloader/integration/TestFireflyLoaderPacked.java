package com.aerospike.firefly.bulkloader.integration;

import org.junit.Assert;
import org.junit.Test;

public class TestFireflyLoaderPacked extends TestFireflyLoaderBase {
    final private static String DATA_MODEL = "packed";

    @Test
    public void testDataModelInitializedCorrectly() {
        Assert.assertEquals(DATA_MODEL, graph.getDataModel());
    }

    @Override
    protected String getDataModel() {
        return DATA_MODEL;
    }
}
