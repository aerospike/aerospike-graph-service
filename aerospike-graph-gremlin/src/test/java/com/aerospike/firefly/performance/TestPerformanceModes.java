package com.aerospike.firefly.performance;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestPerformanceModes {
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();
    private static final String[] DEFAULT_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000"};
    private static final String[] THROUGHPUT_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.performance-mode=throughput"};
    private static final String[] INVALID_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.performance-mode=foo"};
    private static final String[] LATENCY_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.performance-mode=latency"};
    private static final String[] THROUGHPUT_AND_GREMLINPOOL_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.performance-mode=throughput",
            "aerospike.graph-service.gremlinPool=8"};
    private static final String[] THROUGHPUT_AND_THREADPOOLWORKER_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.performance-mode=throughput",
            "aerospike.graph-service.threadPoolWorker=8"};
    private static final String[] THROUGHPUT_AND_GREMLINPOOL_AND_THREADPOOLWORKER_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.performance-mode=throughput",
            "aerospike.graph-service.threadPoolWorker=8",
            "aerospike.graph-service.gremlinPool=8"};
    private static final String[] GREMLINPOOL_NO_PERFORMANCE_MODE_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.gremlinPool=21"};
    private static final String[] THREADPOOLWORKER_NO_PERFORMANCE_MODE_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.threadPoolWorker=11"};
    private static final String[] GREMLINPOOL_AND_THREADPOOLWORKER_NO_PERFORMANCE_MODE_ENVIRONMENT_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph-service.gremlinPool=51",
            "aerospike.graph-service.threadPoolWorker=41"};

    public static int getExpectedGremlinPoolThroughput() {
        return Math.max(Runtime.getRuntime().availableProcessors() * 4, 1);
    }

    public static int getExpectedGremlinPoolLatency() {
        return Math.max(Runtime.getRuntime().availableProcessors(), 1);
    }

    public static int getExpectedThreadPoolWorkerCountThroughput() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
    }

    public static int getExpectedThreadPoolWorkerCountLatency() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 4, 1);
    }

    public void testPerformanceMode(final String[] environmentVariables,
                                    final int expectedGremlinPool,
                                    final int expectedThreadPoolWorker,
                                    boolean isDefault) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundPerfModeSetting = false;
        boolean gremlinPoolFound = false;
        boolean threadPoolWorkerFound = false;
        boolean isDefaultFound = false;
        for (final String line : log) {
            System.out.println(line);
            if (line.equals("Defaulting 'aerospike.graph-service.performance-mode' to 'throughput'.")) {
                Assert.assertTrue(isDefault);
                isDefaultFound = true;
            } else if (line.contains("'aerospike.graph-service.performance-mode' is set to ")) {
                foundPerfModeSetting = true;
            } else if (line.startsWith("gremlinPool:")) {
                Assert.assertEquals(String.valueOf(expectedGremlinPool), line.split(":")[1].trim());
                gremlinPoolFound = true;
            } else if (line.startsWith("threadPoolWorker:")) {
                Assert.assertEquals(String.valueOf(expectedThreadPoolWorker), line.split(":")[1].trim());
                threadPoolWorkerFound = true;
            }
        }
        Assert.assertTrue(gremlinPoolFound);
        Assert.assertTrue(threadPoolWorkerFound);
        Assert.assertTrue(foundPerfModeSetting);
        if (isDefault) {
            Assert.assertTrue(isDefaultFound);
        }
    }

    public void testNoPerformanceMode(final String[] environmentVariables,
                                    final int expectedGremlinPool,
                                    final int expectedThreadPoolWorker) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundPerfModeSetting = false;
        boolean defaultPerfModeFound = false;
        boolean gremlinPoolFound = false;
        boolean threadPoolWorkerFound = false;
        for (final String line : log) {
            System.out.println(line);
            if (line.equals("Defaulting 'aerospike.graph-service.performance-mode' to 'throughput'.")) {
                defaultPerfModeFound = true;
            } else if (line.contains("'aerospike.graph-service.performance-mode' is set to ")) {
                foundPerfModeSetting = true;
            } else if (line.startsWith("gremlinPool:")) {
                if (expectedGremlinPool == -1) {
                    Assert.fail("Gremlin pool should not be set.");
                }
                Assert.assertEquals(String.valueOf(expectedGremlinPool), line.split(":")[1].trim());
                gremlinPoolFound = true;
            } else if (line.startsWith("threadPoolWorker:")) {
                if (expectedThreadPoolWorker == -1) {
                    Assert.fail("Thread pool worker should not be set.");
                }
                threadPoolWorkerFound = true;
            }
        }
        Assert.assertEquals(expectedGremlinPool != -1, gremlinPoolFound);
        Assert.assertEquals(expectedThreadPoolWorker != -1, threadPoolWorkerFound);
        Assert.assertFalse(foundPerfModeSetting);
        Assert.assertFalse(defaultPerfModeFound);
    }

    @Test
    public void testDefaultPerformanceMode() throws InterruptedException {
        // Should default to throughput mode.
        testPerformanceMode(DEFAULT_ENVIRONMENT_VARIABLES,
                getExpectedGremlinPoolThroughput(),
                getExpectedThreadPoolWorkerCountThroughput(),
                true);
    }

    @Test
    public void testDefaultPerformanceModeSetThroughput() throws InterruptedException {
        testPerformanceMode(THROUGHPUT_ENVIRONMENT_VARIABLES,
                getExpectedGremlinPoolThroughput(),
                getExpectedThreadPoolWorkerCountThroughput(),
                false);
    }

    @Test
    public void testDefaultPerformanceModeSetLatency() throws InterruptedException {
        testPerformanceMode(LATENCY_ENVIRONMENT_VARIABLES,
                getExpectedGremlinPoolLatency(),
                getExpectedThreadPoolWorkerCountLatency(),
                false);
    }

    public static void testInvalidPerformanceConfig(final String[] environmentVariables,
                                                    final String expectedErrorMessage) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundErrorMessage = false;
        for (final String line : log) {
            System.out.println(line);
            if (line.contains(expectedErrorMessage)) {
                foundErrorMessage = true;
                break;
            }
        }
        Assert.assertTrue(foundErrorMessage);
    }

    @Test
    public void testInvalidPerformanceMode() throws InterruptedException {
        testInvalidPerformanceConfig(INVALID_ENVIRONMENT_VARIABLES,
                "Invalid value for 'aerospike.graph-service.performance-mode'. " +
                        "Valid values are 'throughput' and 'latency'. Provided value is 'foo'.");
    }

    @Test
    public void testGremlinPoolAndPerformanceMode() throws InterruptedException {
        testInvalidPerformanceConfig(THROUGHPUT_AND_GREMLINPOOL_ENVIRONMENT_VARIABLES,
                "Cannot set 'aerospike.graph-service.threadPoolWorker' or " +
                        "'aerospike.graph-service.gremlinPool' when using 'aerospike.graph-service.performance-mode'");
    }

    @Test
    public void testThreadPoolWorkerAndPerformanceMode() throws InterruptedException {
        testInvalidPerformanceConfig(THROUGHPUT_AND_THREADPOOLWORKER_ENVIRONMENT_VARIABLES,
                "Cannot set 'aerospike.graph-service.threadPoolWorker' or " +
                        "'aerospike.graph-service.gremlinPool' when using 'aerospike.graph-service.performance-mode'");
    }

    @Test
    public void testGremlinPoolAndThreadPoolWorkerAndPerformanceMode() throws InterruptedException {
        testInvalidPerformanceConfig(THROUGHPUT_AND_GREMLINPOOL_AND_THREADPOOLWORKER_ENVIRONMENT_VARIABLES,
                "Cannot set 'aerospike.graph-service.threadPoolWorker' or " +
                        "'aerospike.graph-service.gremlinPool' when using 'aerospike.graph-service.performance-mode'");

    }

    @Test
    public void testGremlinPoolNoPerformanceMode() throws InterruptedException {
        testNoPerformanceMode(GREMLINPOOL_NO_PERFORMANCE_MODE_ENVIRONMENT_VARIABLES, 21, -1);
    }

    @Test
    public void testThreadPoolWorkerNoPerformanceMode() throws InterruptedException {
        testNoPerformanceMode(THREADPOOLWORKER_NO_PERFORMANCE_MODE_ENVIRONMENT_VARIABLES, -1, 11);
    }

    @Test
    public void testGremlinPoolAndThreadPoolWorkerNoPerformanceMode() throws InterruptedException {
        testNoPerformanceMode(GREMLINPOOL_AND_THREADPOOLWORKER_NO_PERFORMANCE_MODE_ENVIRONMENT_VARIABLES, 51, 41);
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
