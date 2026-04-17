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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.process.call.metadata.MetadataServiceUsage.MILLISECONDS_TO_HOURS;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.USAGE_STATS_UPDATE_INTERVAL;
import static org.junit.Assert.fail;

public class FireflyUsageStatsCallTest {
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

    @Test
    public void testSingleFirefly() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {

            // Wait 11 seconds so we can update.
            Thread.sleep(11000);

            // Get test vcpu count.
            final Long testVcpuCount = (long) Runtime.getRuntime().availableProcessors();

            // Call usage stats api to get usage stats.
            final List<Object> usageStatsList = graph.traversal().call("aerospike.graph.admin.metadata.usage").toList();
            Assert.assertEquals(1, usageStatsList.size());
            final Map<String, Object> usageStats = (Map<String, Object>) usageStatsList.get(0);
            final List<Map<String, Object>> rawUsageStats = (List<Map<String, Object>>) usageStats.get("raw");

            // Raw should be list of map.
            Assert.assertTrue(usageStats.get("raw") instanceof List);
            Assert.assertEquals(1, ((List<?>) usageStats.get("raw")).size());

            // Vcpu count of raw should be same of test vcpu count.
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(0).get("vcpus"));

            // Max mem of raw should be same of max mem of test.
            Assert.assertEquals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024), rawUsageStats.get(0).get("memory-gb"));

            // Compare expected and vcpu-yrs.
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") > testVcpuCount * (8000f / (MILLISECONDS_TO_HOURS)));
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") < testVcpuCount * (12000f / MILLISECONDS_TO_HOURS));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testUsageDeprecated() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {

            // Wait 11 seconds so we can update.
            Thread.sleep(11000);

            // Get test vcpu count.
            final Long testVcpuCount = (long) Runtime.getRuntime().availableProcessors();

            // Call usage stats api to get usage stats.
            final List<Object> usageStatsList = graph.traversal().call("usage-stats").toList();
            Assert.assertEquals(1, usageStatsList.size());
            final Map<String, Object> usageStats = (Map<String, Object>) usageStatsList.get(0);
            final List<Map<String, Object>> rawUsageStats = (List<Map<String, Object>>) usageStats.get("raw");

            // Raw should be list of map.
            Assert.assertTrue(usageStats.get("raw") instanceof List);
            Assert.assertEquals(1, ((List<?>) usageStats.get("raw")).size());

            // Vcpu count of raw should be same of test vcpu count.
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(0).get("vcpus"));

            // Max mem of raw should be same of max mem of test.
            Assert.assertEquals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024), rawUsageStats.get(0).get("memory-gb"));

            // Compare expected and vcpu-yrs.
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") > testVcpuCount * (8000f / (MILLISECONDS_TO_HOURS)));
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") < testVcpuCount * (12000f / MILLISECONDS_TO_HOURS));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSinceParameterNull() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {

            // Call usage stats api to get usage stats.
            try {
                graph.traversal().call("aerospike.graph.admin.metadata.usage").with("since", null).toList();
                fail("Expected call to aerospike.graph.admin.metadata.usage with 'null' to fail");
            } catch (final IllegalArgumentException e) {
                Assert.assertEquals("Illegal arguments provided to 'aerospike.graph.admin.metadata.usage'.\n" +
                        "\tExpected either no arguments provided or 'since' with a value in format 'yyyy-MM-dd'.\n" +
                        "\tProvided arguments: '{since=null}'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"aerospike.graph.admin.metadata.usage\").next();\n" +
                        "\t\t\tor\n" +
                        "\t\tg.call(\"aerospike.graph.admin.metadata.usage\").with(\"since\", \"1993-03-30\").next();", e.getMessage());
            }
        }
    }

    @Test
    public void testSinceParameterNonStringDate() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {

            // Call usage stats api to get usage stats.
            try {
                graph.traversal().call("aerospike.graph.admin.metadata.usage").with("since", 1).toList();
                fail("Expected call to aerospike.graph.admin.metadata.usage with 'null' to fail");
            } catch (final IllegalArgumentException e) {
                Assert.assertEquals("Illegal arguments provided to 'aerospike.graph.admin.metadata.usage'.\n" +
                "\tExpected either no arguments provided or 'since' with a value in format 'yyyy-MM-dd'.\n" +
                        "\tProvided arguments: '{since=1}'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"aerospike.graph.admin.metadata.usage\").next();\n" +
                        "\t\t\tor\n" +
                        "\t\tg.call(\"aerospike.graph.admin.metadata.usage\").with(\"since\", \"1993-03-30\").next();", e.getMessage());
            }
        }
    }

    @Test
    public void testSinceParameterPreviousDate() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        final String previousDay = "2020-01-01";
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            // Wait 11 seconds so we can update.
            Thread.sleep(11000);

            // Get test vcpu count.
            final Long testVcpuCount = (long) Runtime.getRuntime().availableProcessors();

            // Call usage stats api to get usage stats.
            final List<Object> usageStatsList = graph.traversal().call("aerospike.graph.admin.metadata.usage").with("since", previousDay).toList();
            Assert.assertEquals(1, usageStatsList.size());
            final Map<String, Object> usageStats = (Map<String, Object>) usageStatsList.get(0);
            final List<Map<String, Object>> rawUsageStats = (List<Map<String, Object>>) usageStats.get("raw");

            // Raw should be list of map.
            Assert.assertTrue(usageStats.get("raw") instanceof List);
            Assert.assertEquals(1, ((List<?>) usageStats.get("raw")).size());

            // Vcpu count of raw should be same of test vcpu count.
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(0).get("vcpus"));

            // Max mem of raw should be same of max mem of test.
            Assert.assertEquals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024), rawUsageStats.get(0).get("memory-gb"));

            // Compare expected and vcpu-yrs.
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") > testVcpuCount * (8000f / (MILLISECONDS_TO_HOURS)));
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") < testVcpuCount * (12000f / MILLISECONDS_TO_HOURS));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSinceParameterFutureDate() {
        // 5 seconds to update
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        final String futureDay = "2030-01-01";
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {

            // Wait 11 seconds so we can update.
            Thread.sleep(11000);

            // Get test vcpu count.
            final Long testVcpuCount = (long) Runtime.getRuntime().availableProcessors();

            // Call usage stats api to get usage stats.
            final List<Object> usageStatsList = graph.traversal().call("aerospike.graph.admin.metadata.usage").with("since", futureDay).toList();
            Assert.assertEquals(1, usageStatsList.size());
            final Map<String, Object> usageStats = (Map<String, Object>) usageStatsList.get(0);
            final List<Map<String, Object>> rawUsageStats = (List<Map<String, Object>>) usageStats.get("raw");

            // Raw should be list of map.
            Assert.assertTrue(usageStats.get("raw") instanceof List);
            Assert.assertEquals(1, ((List<?>) usageStats.get("raw")).size());

            // Vcpu count of raw should be same of test vcpu count.
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(0).get("vcpus"));

            // Max mem of raw should be same of max mem of test.
            Assert.assertEquals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024), rawUsageStats.get(0).get("memory-gb"));

            // Compare expected and vcpu-yrs.
            Assert.assertEquals((Double) 0.0, (Double) usageStats.get("total-vcpu-hours"));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMultiLocalFireflyProducesSeveralResults() {
        FireflyGraph graph1 = null;
        FireflyGraph graph2 = null;
        FireflyGraph graph3 = null;
        FireflyGraph graph4 = null;
        FireflyGraph graph5 = null;
        try {
            // 5 seconds to update
            CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
            graph1 = FireflyGraph.open(CONFIG);
            graph2 = FireflyGraph.open(CONFIG);
            graph3 = FireflyGraph.open(CONFIG);
            graph4 = FireflyGraph.open(CONFIG);
            graph5 = FireflyGraph.open(CONFIG);

            // Wait 11 seconds so we can update.
            Thread.sleep(11000);

            // Get test vcpu count.
            final Long testVcpuCount = (long) Runtime.getRuntime().availableProcessors();

            // Call usage stats api to get usage stats.
            final List<Object> usageStatsList = graph1.traversal().call("aerospike.graph.admin.metadata.usage").toList();
            Assert.assertEquals(1, usageStatsList.size());
            final Map<String, Object> usageStats = (Map<String, Object>) usageStatsList.get(0);
            final List<Map<String, Object>> rawUsageStats = (List<Map<String, Object>>) usageStats.get("raw");

            // Raw should be list of map.
            Assert.assertTrue(usageStats.get("raw") instanceof List);
            Assert.assertEquals(1, ((List<?>) usageStats.get("raw")).size());

            // Vcpu count of raw should be same of test vcpu count.
            Assert.assertEquals(testVcpuCount, rawUsageStats.get(0).get("vcpus"));

            // Max mem of raw should be same of max mem of test.
            Assert.assertEquals(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024), rawUsageStats.get(0).get("memory-gb"));

            // Compare expected vcpu-yrs.
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") > testVcpuCount * (8000f / MILLISECONDS_TO_HOURS));
            Assert.assertTrue((Double) usageStats.get("total-vcpu-hours") < testVcpuCount * (12000f / MILLISECONDS_TO_HOURS));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (graph1 != null)
                graph1.close();
            if (graph2 != null)
                graph2.close();
            if (graph3 != null)
                graph3.close();
            if (graph4 != null)
                graph4.close();
            if (graph5 != null)
                graph5.close();
        }
    }
}
