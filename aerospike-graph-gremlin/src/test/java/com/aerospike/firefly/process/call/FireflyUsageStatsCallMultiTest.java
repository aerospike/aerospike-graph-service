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

package com.aerospike.firefly.process.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.process.call.metadata.MetadataServiceUsage.MILLISECONDS_TO_HOURS;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.USAGE_STATS_UPDATE_INTERVAL;

public class FireflyUsageStatsCallMultiTest {
    private static final long USAGE_STATS_MIN_ELAPSED_MS = 5000L;
    private static final long USAGE_STATS_WAIT_MS = 15000L;
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    @Before
    public void cleanUsageStats() {
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            Thread.sleep(1);
            graph.getBaseGraph().truncate(null, graph.getBaseGraph().getConfig().usageStatsSet, null);
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void cleanUp() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            graph.getUsageStats().restartUsageStats(graph.getBaseGraph());
        }
    }

    @Test
    public void testBackgroundDockerAddsToGraph() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {

            // Wait for at least one usage-stats update interval on shared CI runners.
            Thread.sleep(USAGE_STATS_WAIT_MS);

            // Get test vcpu count.
            final Long testVcpuCount = (long) Runtime.getRuntime().availableProcessors();

            // Call usage stats api to get usage stats.
            final List<Object> usageStatsList = graph.traversal().call("aerospike.graph.admin.metadata.usage").toList();
            Assert.assertEquals(1, usageStatsList.size());
            final Map<String, Object> usageStats = (Map<String, Object>) usageStatsList.get(0);
            final List<Map<String, Object>> rawUsageStats = (List<Map<String, Object>>) usageStats.get("raw");

            // Raw should be list of map.
            Assert.assertTrue(usageStats.get("raw") instanceof List);
            // this test on GHA runs with an additional FireFly server running in docker,
            // so expected result is 1 for local and 2 for GHA
            Assert.assertEquals(2, ((List<?>) usageStats.get("raw")).size());

            // Vcpu count of raw should be same of test vcpu count.
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(0).get("vcpus"));
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(1).get("vcpus"));

            // Max mem of raw should be same of max mem of test.
            // Memory of docker container is ~7 GB in GitHub Actions and we get 80% by default - should round to ~5 GB.
            if (rawUsageStats.get(0).get("memory-gb").equals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024))) {
                Assert.assertTrue((long) rawUsageStats.get(1).get("memory-gb") > 4L);
            } else {
                Assert.assertTrue((long) rawUsageStats.get(0).get("memory-gb") > 4L);
                Assert.assertEquals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024), rawUsageStats.get(1).get("memory-gb"));
            }

            // Compare expected vcpu-yrs. We know lower bound since we know minimum time it could be but not upper.
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") > testVcpuCount * (USAGE_STATS_MIN_ELAPSED_MS / (float) MILLISECONDS_TO_HOURS));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
