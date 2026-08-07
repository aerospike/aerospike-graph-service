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

package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static org.junit.Assert.assertEquals;

/**
 * Regression test for the {@code FireflyGraphSummaryUpdater} thread leak (see its {@code close()}).
 * Runs several bulk loads in a loop, in-process, against a local Aerospike instance
 * (e.g. `docker run -p 3000:3000 ... aerospike/aerospike-server`), and asserts after every load that
 * the JVM's "pool-N-thread" count has returned to baseline.
 */
public class TestBulkLoaderThreadLeak {
    private static final Path LOADER_CONFIG = Path.of("src/test/resources/conf/packed/thread-leak.properties");
    private static final int ITERATIONS = 3;
    private static final Pattern POOL_THREAD_NAME = Pattern.compile("^(pool-\\d+-thread)-\\d+$");
    private static final String LEAK_SIGNATURE = "pool-N-thread (leak signature)";

    private final Configuration config;

    public TestBulkLoaderThreadLeak() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void noPoolThreadLeakAcrossRepeatedBulkLoads() throws InterruptedException {
        final long baselinePoolThreads = poolThreadTotal(threadCensus());

        for (int i = 1; i <= ITERATIONS; i++) {
            try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
                final GraphTraversalSource g = fireflyGraph.traversal();
                g.V().drop().iterate();
                RecoveryUtil.truncate(fireflyGraph);
                g.call("aerospike.graphloader.admin.bulk-load.load")
                        .with("aerospike.graphloader.config", LOADER_CONFIG.toString())
                        .iterate();
                waitForBulkLoad(g);
            }
            Thread.sleep(500);

            final Map<String, Long> census = threadCensus();
            assertEquals("Bulk load #" + i + " leaked \"pool-N-thread\" threads. Census: " + census,
                    baselinePoolThreads, poolThreadTotal(census));
        }
    }

    /**
     * Snapshots all live threads, grouped by name with trailing numeric IDs stripped
     * (e.g. "pool-42-thread-1" and "pool-57-thread-1" both collapse to "pool-N-thread").
     */
    private static Map<String, Long> threadCensus() {
        final Map<String, Long> counts = new TreeMap<>();
        for (final Thread t : Thread.getAllStackTraces().keySet()) {
            final String bucket = bucketName(t.getName());
            counts.merge(bucket, 1L, Long::sum);
        }
        return counts;
    }

    private static String bucketName(final String threadName) {
        final Matcher m = POOL_THREAD_NAME.matcher(threadName);
        if (m.matches()) {
            return LEAK_SIGNATURE;
        }
        return threadName.replaceAll("[0-9]+", "N");
    }

    private static long poolThreadTotal(final Map<String, Long> census) {
        return census.getOrDefault(LEAK_SIGNATURE, 0L);
    }
}
