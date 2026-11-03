/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    private static final long BULK_LOAD_TIMEOUT_MS = 5 * 60 * 1000;

    static public void waitForBulkLoad(final GraphTraversalSource g) {
        final Map<String, Object> status = waitForCompletion(g);
        Assert.assertEquals(BULK_LOAD_STATUS_SUCCESS, status.get(BULK_LOAD_STATUS_KEY));
    }

    static public String waitForBulkLoadFail(final GraphTraversalSource g) {
        final Map<String, Object> status = waitForCompletion(g);
        Assert.assertEquals(BULK_LOAD_STATUS_ERROR, status.get(BULK_LOAD_STATUS_KEY));
        return (String) status.get(BULK_LOAD_EXCEPTION_MESSAGE);
    }

    private static Map<String, Object> waitForCompletion(final GraphTraversalSource g) {
        final long deadline = System.currentTimeMillis() + BULK_LOAD_TIMEOUT_MS;
        Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
        while (!(boolean) status.get(PROGRESS_COMPLETE)) {
            if (System.currentTimeMillis() > deadline) {
                Assert.fail("Timed out after " + BULK_LOAD_TIMEOUT_MS + " ms waiting for bulk load to complete. " +
                        "Last status: " + status);
            }
            try {
                Thread.sleep(200);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            status = (Map<String, Object>) g.call("aerospike.graphloader.admin.bulk-load.status").next();
        }
        return status;
    }
}
