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

package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Bounded polling for tests. Every wait on external state should go through here (or carry its own deadline) so a
 * condition that never becomes true fails the test with a message instead of hanging the CI job.
 */
public final class WaitUtil {
    public static final long DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;
    public static final long DEFAULT_POLL_INTERVAL_MS = 200;

    private WaitUtil() {
    }

    public static void waitUntil(final String description, final BooleanSupplier condition) {
        waitUntil(description, DEFAULT_TIMEOUT_MS, DEFAULT_POLL_INTERVAL_MS, condition);
    }

    public static void waitUntil(final String description, final long timeoutMs, final long pollIntervalMs,
                                 final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                Assert.fail("Timed out after " + timeoutMs + " ms waiting for " + description);
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public static Map<String, Long> vertexIndexStatus(final GraphTraversalSource g, final String propertyKey) {
        return (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                with("property_key", propertyKey).
                with("element_type", "vertex").next();
    }

    public static void waitForVertexIndexesBuilt(final GraphTraversalSource g, final String... propertyKeys) {
        waitUntil("vertex indexes " + Arrays.toString(propertyKeys) + " to finish building",
                () -> Arrays.stream(propertyKeys).allMatch(key -> vertexIndexStatus(g, key).get("percent_complete") >= 100));
    }
}
