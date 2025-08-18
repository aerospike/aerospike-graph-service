package com.aerospike.firefly.bulkloader.integration.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;

import java.util.Map;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_EXCEPTION_MESSAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_ERROR;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_SUCCESS;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.PROGRESS_COMPLETE;

public class BulkLoadTestUtil {

    static public void waitForBulkLoad(final GraphTraversalSource g) {
        Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
        while (!(boolean)status.get(PROGRESS_COMPLETE)) {
            status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
            try {
                Thread.sleep(200);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Assert.assertEquals(BULK_LOAD_STATUS_SUCCESS, status.get(BULK_LOAD_STATUS_KEY));
    }

    static public String waitForBulkLoadFail(final GraphTraversalSource g) {
        Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
        while (!(boolean)status.get(PROGRESS_COMPLETE)) {
            status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
            try {
                Thread.sleep(200);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Assert.assertEquals(BULK_LOAD_STATUS_ERROR, status.get(BULK_LOAD_STATUS_KEY));
        return (String) status.get(BULK_LOAD_EXCEPTION_MESSAGE);
    }
}