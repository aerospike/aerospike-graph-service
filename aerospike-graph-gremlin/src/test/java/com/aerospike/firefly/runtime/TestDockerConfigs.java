package com.aerospike.firefly.runtime;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.DockerUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class TestDockerConfigs {
    private static final Logger LOG = LoggerFactory.getLogger(TestDockerConfigs.class);
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();
    @Rule
    public TestName testName = new TestName();

    public void testDockerImageSettings(final String[] environmentVariables) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundErrorMsg = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Error: 'aerospike.client.clientPolicy.minConnsPerNode' is set to '2' which is greater than 'aerospike.client.clientPolicy.maxConnsPerNode' set to '1'. 'aerospike.client.clientPolicy.minConnsPerNode' must be less than or equal to 'aerospike.client.clientPolicy.maxConnsPerNode'.")) {
                foundErrorMsg = true;
                break;
            }
        }
        Assert.assertTrue(foundErrorMsg);
    }

    @Test
    public void testMinConnPerNodeGreaterThanMaxConnPerNode() throws InterruptedException {
        testDockerImageSettings(new String[]{
                "aerospike.client.host=localhost:3000",
                "aerospike.client.clientPolicy.maxConnsPerNode=1",
                "aerospike.client.clientPolicy.minConnsPerNode=2"});
    }

    @Test
    public void testMultiTenantSettingsEmpty() throws InterruptedException {
        final String[] environmentVariables = new String[]{"aerospike.client.host=localhost:3000"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg0 = false;
        boolean foundMsg1 = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Found named graphs: []")) {
                foundMsg0 = true;
            } else if (line.contains("graph: /opt/conf/aerospike-graph-graph.properties,")) {
                foundMsg1 = true;
            }

            if (foundMsg0 && foundMsg1) {
                break;
            }
        }
        Assert.assertTrue(foundMsg0 && foundMsg1);
    }

    @Test
    public void testMultiTenantSettingsWithEnvVariables() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.graph-service.graphs=graph,modern"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg0 = false;
        boolean foundMsg1 = false;
        boolean foundMsg2 = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Found named graphs: ['graph', 'modern']")) {
                foundMsg0 = true;
            } else if (line.contains("graph: /opt/conf/aerospike-graph-graph.properties,")) {
                foundMsg1 = true;
            } else if (line.contains("modern: /opt/conf/aerospike-graph-modern.properties,")) {
                foundMsg2 = true;
            }

            if (foundMsg0 && foundMsg1 && foundMsg2) {
                break;
            }
        }
        Assert.assertTrue(foundMsg0 && foundMsg1 && foundMsg2);
    }

    @Test
    public void testGraphNameValidation() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.graph-service.graphs=graph,modern!"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Graph name should be within [a-z][A-Z][0-9][-_], but found modern!")) {
                foundMsg = true;
                break;
            }
        }
        Assert.assertTrue(foundMsg);
    }

    @Test
    public void testValidMetricConfiguration() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.graph-service.metrics.slf4jReporter.interval=170000",
                "aerospike.graph-service.metrics.csvReporter.enabled=false"
        };
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        // Maybe there's a better way to do this, but for now this will suffice...
        int linesUntilEnableCheck = Integer.MAX_VALUE;
        int linesUntilIntervalCheck = Integer.MAX_VALUE;
        boolean foundEnable = false;
        boolean foundInterval = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("csvReporter: {")) {
                linesUntilEnableCheck = 1;
            } else if (line.contains("slf4jReporter: {")) {
                linesUntilIntervalCheck = 2;
            }

            if (linesUntilEnableCheck == 0 && line.contains("enabled: false")) {
                foundEnable = true;
            }
            if (linesUntilIntervalCheck == 0 && line.contains("interval: 170000")) {
                foundInterval = true;
            }
            linesUntilEnableCheck--;
            linesUntilIntervalCheck--;
        }
        Assert.assertTrue(foundEnable && foundInterval);
    }

    @Test
    public void testInvalidMetricType() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.graph-service.metrics.simonReporter.interval=170000"
        };
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundErrorMsg = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("simonReporter is not a valid metrics type.")) {
                foundErrorMsg = true;
                break;
            }
        }
        Assert.assertTrue(foundErrorMsg);
    }

    @Test
    public void testInvalidMetricConfigKey() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.graph-service.metrics.slf4jReporter.simon=over9000"
        };
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundErrorMsg = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("simon is not a valid configuration for metrics of type slf4jReporter.")) {
                foundErrorMsg = true;
                break;
            }
        }
        Assert.assertTrue(foundErrorMsg);
    }

    @Test
    public void testGitCommitHashLog() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.2:3000",
                "aerospike.graph-service.graphs=graph,modern"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Built from git commit")) {
                foundMsg = true;
            }

            if (foundMsg) {
                break;
            }
        }
        Assert.assertTrue(foundMsg);
    }

    @Before
    public void beforeEach() {
        LOG.warn("===> Running {} <===", testName.getMethodName());
    }

    @After
    public void afterEach() {
        LOG.warn("===> Finished running {} <===", testName.getMethodName());
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
