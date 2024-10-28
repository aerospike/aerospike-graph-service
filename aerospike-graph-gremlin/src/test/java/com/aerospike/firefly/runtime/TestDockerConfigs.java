package com.aerospike.firefly.runtime;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;

public class TestDockerConfigs {
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    public void testDockerImageSettings(final String[] environmentVariables) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundErrorMsg = false;
        for (final String line : log) {
            System.out.println(line);
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
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.client.clientPolicy.maxConnsPerNode=1",
                "aerospike.client.clientPolicy.minConnsPerNode=2"});
    }

    @Test
    public void testMultiTenantSettingsEmpty() throws InterruptedException {
        final String[] environmentVariables = new String[]{
                "aerospike.client.host=172.17.0.1:3000"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg0 = false;
        boolean foundMsg1 = false;
        for (final String line : log) {
            if (line.contains("Found named graphs: []")) {
                foundMsg0 = true;
            } else if (line.contains("graph: conf/aerospike-graph-graph.properties,")) {
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
                "aerospike.graph-service.named-graphs=graph,modern"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", false, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg0 = false;
        boolean foundMsg1 = false;
        boolean foundMsg2 = false;
        for (final String line : log) {
            if (line.contains("Found named graphs: ['graph', 'modern']")) {
                foundMsg0 = true;
            } else if (line.contains("graph: conf/aerospike-graph-graph.properties,")) {
                foundMsg1 = true;
            } else if (line.contains("modern: conf/aerospike-graph-modern.properties,")) {
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
                "aerospike.graph-service.named-graphs=graph,modern!"};
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundMsg = false;
        for (final String line : log) {
            if (line.contains("Graph name should be within [a-z][A-Z][0-9][-_], but found modern!")) {
                foundMsg = true;
                break;
            }
        }
        Assert.assertTrue(foundMsg);
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
