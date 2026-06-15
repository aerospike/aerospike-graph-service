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

package com.aerospike.firefly.performance;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;

public class TestPerformanceModes {
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    private static final String[] DEFAULT_ENV_VARIABLES = new String[]{
            "aerospike.client.host=host.docker.internal:3000"};
    private static final String[] GREMLIN_POOL_ENV_VARIABLES = new String[]{
            "aerospike.graph-service.gremlinPool=51",
            "aerospike.client.host=host.docker.internal:3000"};
    private static final String[] THREAD_POOL_WORKER_ENV_VARIABLES = new String[]{
            "aerospike.graph-service.threadPoolWorker=41",
            "aerospike.client.host=host.docker.internal:3000"};
    private static final String[] GREMLIN_POOL_AND_THREAD_POOL_WORKER_ENV_VARIABLES = new String[]{
            "aerospike.graph-service.gremlinPool=21",
            "aerospike.graph-service.threadPoolWorker=11",
            "aerospike.client.host=host.docker.internal:3000"};

    public void testDockerImageSettings(final String[] environmentVariables,
                                      final int expectedGremlinPool,
                                      final int expectedThreadPoolWorker) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        String gremlinPool = null;
        String threadPoolWorker = null;
        for (final String line : log) {
            System.out.println(line);
            // String looks like 'Setting gremlinPool to " + str(gremlin_pool) + " and threadPoolWorker to " + str(thread_pool_worker) + "."'
            if (line.startsWith("Setting gremlinPool to ")) {
                // Parse out the gremlinPool and threadPoolWorker values.
                final String[] parts = line.split(" ");
                gremlinPool = parts[3];
                threadPoolWorker = parts[7].replace(".", "");
                break;
            }
        }
        Assert.assertEquals(String.valueOf(expectedGremlinPool), gremlinPool);
        Assert.assertEquals(String.valueOf(expectedThreadPoolWorker), threadPoolWorker);
    }

    @Test
    public void testDefaults() throws InterruptedException {
        testDockerImageSettings(DEFAULT_ENV_VARIABLES, getDefaultGremlinPool(), getDefaultThreadPoolWorkers());
    }

    @Test
    public void testGremlinPool() throws InterruptedException {
        testDockerImageSettings(GREMLIN_POOL_ENV_VARIABLES, 51, getDefaultThreadPoolWorkers());
    }

    @Test
    public void testThreadPoolWorker() throws InterruptedException {
        testDockerImageSettings(THREAD_POOL_WORKER_ENV_VARIABLES, getDefaultGremlinPool(), 41);
    }

    @Test
    public void testGremlinPoolAndThreadPoolWorker() throws InterruptedException {
        testDockerImageSettings(GREMLIN_POOL_AND_THREAD_POOL_WORKER_ENV_VARIABLES, 21, 11);
    }

    public static int getDefaultGremlinPool() {
        return Math.max(Runtime.getRuntime().availableProcessors() * 4, 1);
    }

    public static int getDefaultThreadPoolWorkers() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
