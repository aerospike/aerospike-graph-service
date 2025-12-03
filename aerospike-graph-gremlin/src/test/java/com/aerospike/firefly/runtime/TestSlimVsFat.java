package com.aerospike.firefly.runtime;

import com.aerospike.firefly.util.DockerUtil;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestSlimVsFat {
    final Logger LOG = LoggerFactory.getLogger(TestSlimVsFat.class);
    private static final List<Object> EXPECTED_CALL_STEPS_PHAT = List.of(
            "aerospike.graph.admin.metadata.summary",
            "summary",
            "aerospike.graphloader.admin.bulk-load.errors",
            "get-bulk-load-errors",
            "aerospike.graphloader.admin.bulk-load.error-count",
            "get-bulk-load-error-count",
            "aerospike.graphloader.admin.bulk-load.load",
            "bulk-load",
            "aerospike.graphloader.admin.bulk-load.status",
            "aerospike.graph.admin.metadata.usage",
            "usage-stats",
            "aerospike.graph.admin.index.create",
            "aerospike.graph.admin.index.drop",
            "aerospike.graph.admin.index.list",
            "aerospike.graph.admin.index.status",
            "aerospike.graph.admin.index.cardinality",
            "aerospike.graph.admin.reserved.info",
            "aerospike.graph.admin.metadata.version",
            "aerospike.graph.admin.metadata.config",
            "aerospike.graph.admin.metadata.set-config",
            "aerospike.graph.admin.rbac-jwt.issue-token",
            "aerospike.graph.admin.query.abort");
    private static final List<Object> EXPECTED_CALL_STEPS_SLIM = List.of(
            "aerospike.graph.admin.metadata.summary",
            "summary",
            "aerospike.graph.admin.metadata.usage",
            "usage-stats",
            "aerospike.graph.admin.index.create",
            "aerospike.graph.admin.index.drop",
            "aerospike.graph.admin.index.list",
            "aerospike.graph.admin.index.status",
            "aerospike.graph.admin.index.cardinality",
            "aerospike.graph.admin.reserved.info",
            "aerospike.graph.admin.metadata.version",
            "aerospike.graph.admin.metadata.config",
            "aerospike.graph.admin.metadata.set-config",
            "aerospike.graph.admin.rbac-jwt.issue-token",
            "aerospike.graph.admin.query.abort");
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    private static final String[] DEFAULT_ENV_VARIABLES = new String[]{
             "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph.auto.preheat.enabled=false",
    };


    private static final String[] DEFAULT_ENV_VARIABLES_PREHEAT = new String[]{"aerospike.client.host=172.17.0.1:3000"};

    public void testFatDockerImageSettings(final String[] environmentVariables) throws InterruptedException {
        testDockerImageSettings("firefly", environmentVariables);
    }

    public void testSlimDockerImageSettings(final String[] environmentVariables) throws InterruptedException {
        testDockerImageSettings("firefly-slim", environmentVariables);
    }

    public void testDockerImageSettings(final String imageName, final String[] environmentVariables) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom(imageName, true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundSuccess = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Channel started at port 8182.")) {
                foundSuccess = true;
            }
        }
        Assert.assertTrue(foundSuccess);
    }

    public void testDockerImageSettingsTmp(final String dockerImage, final String[] environmentVariables) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom(dockerImage, true, 1, environmentVariables);
        Assert.assertFalse(DOCKER_UTIL.checkTmpFileExists(containerId));
        Thread.sleep(25 * 1000);
        Assert.assertTrue(DOCKER_UTIL.checkTmpFileExists(containerId));
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundSuccess = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Channel started at port 8182.")) {
                foundSuccess = true;
            } else if (line.contains("Warmup is complete")) {
                foundSuccess = true;
            }
        }
        Assert.assertTrue(foundSuccess);
    }

    @Test
    public void testSlimCallList() throws InterruptedException {
        LOG.warn("=== Running testSlimCallList ===");
        testSlimDockerImageSettings(DEFAULT_ENV_VARIABLES);
        final DriverRemoteConnection connection = DriverRemoteConnection.using("localhost", 8182);
        final GraphTraversalSource g = traversal().withRemote(connection);
        final List<Object> list = g.call("--list").toList();
        Assert.assertEquals(new HashSet<>(EXPECTED_CALL_STEPS_SLIM), new HashSet<>(list));
    }

    @Test
    public void testCallList() throws InterruptedException {
        LOG.warn("=== Running testCallList ===");
        testFatDockerImageSettings(DEFAULT_ENV_VARIABLES);
        final DriverRemoteConnection connection = DriverRemoteConnection.using("localhost", 8182);
        final GraphTraversalSource g = traversal().withRemote(connection);
        final List<Object> list = g.call("--list").toList();
        Assert.assertEquals(new HashSet<>(EXPECTED_CALL_STEPS_PHAT), new HashSet<>(list));
    }

    @Test
    public void testDefaultsTmpExistsNoPreheat() throws InterruptedException {
        LOG.warn("=== Running testDefaultsTmpExistsNoPreheat ===");
        testDockerImageSettingsTmp("firefly", DEFAULT_ENV_VARIABLES);
    }

    @Test
    public void testDefaultsTmpExistsWithPreheat() throws InterruptedException {
        LOG.warn("=== Running testDefaultsTmpExistsWithPreheat ===");
        testDockerImageSettingsTmp("firefly", DEFAULT_ENV_VARIABLES_PREHEAT);
    }

    @Test
    public void testSlimDefaultTmpExistsNoPreheat() throws InterruptedException {
        LOG.warn("=== Running testSlimDefaultTmpExistsNoPreheat ===");
        testDockerImageSettingsTmp("firefly-slim", DEFAULT_ENV_VARIABLES);
    }

    @Test
    public void testSlimDefaultTmpExistsWithPreheat() throws InterruptedException {
        LOG.warn("=== Running testSlimDefaultTmpExistsWithPreheat ===");
        testDockerImageSettingsTmp("firefly-slim", DEFAULT_ENV_VARIABLES_PREHEAT);
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
