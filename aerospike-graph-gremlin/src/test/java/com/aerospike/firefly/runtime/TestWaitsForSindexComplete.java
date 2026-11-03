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

package com.aerospike.firefly.runtime;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestWaitsForSindexComplete {
    private static final int THREAD_COUNT = 8;
    private static final int STRING_LENGTH = 250;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final Random RANDOM = new Random();
    private static final int INSERT_COUNT = 100000;

    @After
    public void tearDown() {
        // Drop the str1-str5 indexes and the loaded vertices so later test classes see a clean namespace.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    @Test
    public void testWaitsForSindexComplete() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Drop graph.
            graph.getBaseGraph().dropGraphIndices();
            graph.traversal().V().drop().iterate();

            // Load graph.
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            try {
                loadGraph(graph, executorService);
            } finally {
                // Writers left running after a timeout would keep inserting into later test classes.
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Create sindex in background on the property and quickly query it.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, "str1,str2,str3,str4,str5");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            long count = graph.traversal().V().has("str5", "barazfvsad").count().next();

            // Test is only valid if sindex is not ready yet.
            final Map<String, Long> info = (Map<String, Long>) graph.traversal().call("aerospike.graph.admin.index.status").
                    with("property_key", "str5").
                    with("element_type", "vertex").next();
            Assert.assertTrue(info.get("percent_complete") < 100L);
        }
    }

    private static void loadGraph(final FireflyGraph graph, final ExecutorService executorService)
            throws InterruptedException {
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < INSERT_COUNT && !Thread.currentThread().isInterrupted(); j++) {
                    final StringBuilder stringBuilder = new StringBuilder();
                    while (stringBuilder.length() < STRING_LENGTH) {
                        int idx = (int) (RANDOM.nextFloat() * CHARACTERS.length());
                        stringBuilder.append(CHARACTERS.charAt(idx));
                    }
                    graph.traversal().addV("vertex").
                            property("str1", stringBuilder.toString()).
                            property("str2", stringBuilder.toString()).
                            property("str3", stringBuilder.toString()).
                            property("str4", stringBuilder.toString()).
                            property("str5", stringBuilder.toString()).
                            iterate();
                }
            });
        }

        executorService.shutdown();
        boolean completed = executorService.awaitTermination(2, TimeUnit.MINUTES);
        if (!completed) {
            throw new RuntimeException("Failed to complete within 2 minutes");
        }
    }
}
