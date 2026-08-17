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
            "aerospike.graph.admin.compound-index.create",
            "aerospike.graph.admin.compound-index.drop",
            "aerospike.graph.admin.compound-index.list",
            "aerospike.graph.admin.compound-index.status",
            "aerospike.graph.admin.reserved.info",
            "aerospike.graph.admin.metadata.version",
            "aerospike.graph.admin.metadata.config",
            "aerospike.graph.admin.metadata.set-config",
            "aerospike.graph.admin.rbac-jwt.issue-token",
            "aerospike.graph.admin.query.abort",
            "aerospike.graph.admin.cache.reset",
            "aerospike.graph.admin.cache.set-mode",
            "aerospike.graph.admin.cache.status");
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
            "aerospike.graph.admin.compound-index.create",
            "aerospike.graph.admin.compound-index.drop",
            "aerospike.graph.admin.compound-index.list",
            "aerospike.graph.admin.compound-index.status",
            "aerospike.graph.admin.reserved.info",
            "aerospike.graph.admin.metadata.version",
            "aerospike.graph.admin.metadata.config",
            "aerospike.graph.admin.metadata.set-config",
            "aerospike.graph.admin.rbac-jwt.issue-token",
            "aerospike.graph.admin.query.abort",
            "aerospike.graph.admin.cache.reset",
            "aerospike.graph.admin.cache.set-mode",
            "aerospike.graph.admin.cache.status");
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    private static final String[] DEFAULT_ENV_VARIABLES = new String[]{
             "aerospike.client.host=host.docker.internal:3000",
            "aerospike.graph.auto.preheat.enabled=false",
    };


    private static final String[] DEFAULT_ENV_VARIABLES_PREHEAT = new String[]{"aerospike.client.host=host.docker.internal:3000"};

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

    public void testDockerImageSettingsTmp(final String dockerImage,
                                            final String[] environmentVariables,
                                            final boolean expectWarmup) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom(dockerImage, true, 1, environmentVariables);
        Assert.assertTrue("Container did not become ready in time.",
                DOCKER_UTIL.waitForTmpFile(containerId, 30));
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundSuccess = false;
        boolean foundWarmup = false;
        for (final String line : log) {
            LOG.warn(line);
            if (line.contains("Channel started at port 8182.")) {
                foundSuccess = true;
            } else if (line.contains("Warmup is complete")) {
                foundSuccess = true;
                foundWarmup = true;
            }
        }
        Assert.assertTrue(foundSuccess);
        Assert.assertEquals("Unexpected warmup behavior.", expectWarmup, foundWarmup);
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
        testDockerImageSettingsTmp("firefly", DEFAULT_ENV_VARIABLES, false);
    }

    @Test
    public void testDefaultsTmpExistsWithPreheat() throws InterruptedException {
        LOG.warn("=== Running testDefaultsTmpExistsWithPreheat ===");
        testDockerImageSettingsTmp("firefly", DEFAULT_ENV_VARIABLES_PREHEAT, true);
    }

    @Test
    public void testSlimDefaultTmpExistsNoPreheat() throws InterruptedException {
        LOG.warn("=== Running testSlimDefaultTmpExistsNoPreheat ===");
        testDockerImageSettingsTmp("firefly-slim", DEFAULT_ENV_VARIABLES, false);
    }

    @Test
    public void testSlimDefaultTmpExistsWithPreheat() throws InterruptedException {
        LOG.warn("=== Running testSlimDefaultTmpExistsWithPreheat ===");
        testDockerImageSettingsTmp("firefly-slim", DEFAULT_ENV_VARIABLES_PREHEAT, true);
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
