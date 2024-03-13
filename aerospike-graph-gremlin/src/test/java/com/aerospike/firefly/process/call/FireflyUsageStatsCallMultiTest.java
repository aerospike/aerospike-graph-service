package com.aerospike.firefly.process.call;

import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.process.call.usage.FireflyUsageStatsServiceFactory.HOURS_TO_YEARS;
import static com.aerospike.firefly.process.call.usage.FireflyUsageStatsServiceFactory.MILLISECONDS_TO_HOURS;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.USAGE_STATS_UPDATE_INTERVAL;

public class FireflyUsageStatsCallMultiTest {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    @Before
    public void cleanUsageStats() {
        CONFIG.setProperty(USAGE_STATS_UPDATE_INTERVAL.toLowerCase(), "5000");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            Thread.sleep(1);
            graph.getBaseGraph().getClient().truncate(null,
                    graph.getBaseGraph().namespace, graph.getBaseGraph().USAGE_STATS_SET, null);
            Thread.sleep(1);
            FireflyUsageStats.restartUsageStats(graph.getBaseGraph());
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void cleanUp() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            FireflyUsageStats.restartUsageStats(graph.getBaseGraph());
        }
    }

    @Test
    public void testBackgroundDockerAddsToGraph() {
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
            Assert.assertTrue((Double) usageStats.get("total-vcpu") > 2 * testVcpuCount * (8000f / (MILLISECONDS_TO_HOURS * HOURS_TO_YEARS)));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
