package com.aerospike.firefly.util;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DockerUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DockerUtil.class);
    private static final int DEFAULT_PORT = 8182;
    public static final String AEROSPIKE_GRAPH_SERVICE = "aerospike/aerospike-graph-service";
    private final DockerClient dockerClient;
    private final Map<String, DockerInfo> dockerImageTagToContainerId = new HashMap<>();

    private class DockerInfo {
        final String containerId;
        final Integer portRemap;

        DockerInfo(final String containerId, final Integer portRemap) {
            this.containerId = containerId;
            this.portRemap = portRemap;
        }
    }

    /**
     * Constructor initialized docker client so that docker commands can be executed. Function will
     * assert or throw if conditions of success are not met.
     */
    public DockerUtil() {
        // Create default config for docker client.
        final DockerClientConfig dockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().
                build();

        // Create docker http client with default config.
        final DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(dockerClientConfig.getDockerHost())
                .sslConfig(dockerClientConfig.getSSLConfig())
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build();

        // Create ping request to test http client.
        final DockerHttpClient.Request request = DockerHttpClient.Request.builder()
                .method(DockerHttpClient.Request.Method.GET)
                .path("/_ping")
                .build();

        // Test http client via ping request.
        try (final DockerHttpClient.Response response = httpClient.execute(request)) {
            Assert.assertEquals(response.getStatusCode(), 200);
            Assert.assertEquals(IOUtils.toString(response.getBody()), "OK");
        } catch (IOException e) {
            Assert.fail("Docker is not running or not reachable. Please start docker and try again. " + e.getMessage());
            throw new RuntimeException(e);
        }

        // Ping request succeeded, create docker client with http client.
        this.dockerClient = DockerClientImpl.getInstance(dockerClientConfig, httpClient);
    }

    public synchronized Integer startDockerImage(final String dockerImage, final String tag) {
        final List<Image> images = dockerClient.listImagesCmd().exec();
        for (final Image image : images) {
            // Check if image is already an image on the system of the same tag.
            if (image.getRepoTags().length == 1 && image.getRepoTags()[0].equals(dockerImage + ":" + tag)) {
                // We want to try to kill the image and remove it.
                try {
                    dockerClient.killContainerCmd(image.getId()).exec();
                } catch (Exception ignored) {
                }
                try {
                    dockerClient.removeImageCmd(image.getId()).withForce(true).exec();
                } catch (Exception ignored) {
                }
                break;
            }
        }

        final String dockerImageTag = "test-graph-" + tag;
        // If there is a container of the same name, remove it.
        try {
            dockerClient.removeContainerCmd(dockerImageTag).withForce(true).exec();
        } catch (Exception ignored) {
        }

        // Pull the image in case we do not already have it.
        try {
            dockerClient.pullImageCmd("aerospike/aerospike-graph-service").withTag(tag).start().awaitCompletion();
        } catch (final Exception e) {
            LOG.error("Failed to pull docker image: " + dockerImage + ":" + tag, e);
            throw new RuntimeException(e);
        }

        // Create port bindings and expose port 8182.
        final ExposedPort tcp8182 = ExposedPort.tcp(DEFAULT_PORT);
        final Ports portBindings = new Ports();
        portBindings.bind(tcp8182, Ports.Binding.bindPort(DEFAULT_PORT));

        // Create container with name, port, and environment set.
        final String containerId = dockerClient.createContainerCmd(dockerImage + ":" + tag)
                .withName(dockerImageTag)
                .withExposedPorts(tcp8182)
                .withHostConfig(new HostConfig().withPortBindings(portBindings))
                .withEnv("aerospike.client.host=172.17.0.1:3000")
                .exec().getId();

        // Start the container.
        dockerClient.startContainerCmd(containerId).exec();
        dockerImageTagToContainerId.put(containerId, new DockerInfo(dockerImageTag, 8182));

        // Wait 30 seconds for the container to be fully up.
        try {
            Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Need to check if the container is running.
        InspectContainerResponse.ContainerState containerState = dockerClient.inspectContainerCmd(containerId).exec().getState();

        if (Boolean.FALSE.equals(containerState.getRunning())) {
            throw new RuntimeException("Error failed to start container " + dockerImage + ":" + tag +
                    " under image name test-graph-" + tag + ". Container state: " + containerState.getStatus());
        }

        return DEFAULT_PORT;
    }

    public synchronized String startDockerImageCustom(final String dockerImage,
                                                      final boolean expectException,
                                                      final String... environmentVariables) {
        return startDockerImageCustom(dockerImage, expectException, 20, environmentVariables);
    }

    public synchronized String startDockerImageCustom(final String dockerImage,
                                                      final boolean expectException,
                                                      final int waitTimeSeconds,
                                                      final String... environmentVariables) {
        final List<Image> images = dockerClient.listImagesCmd().exec();
        for (final Image image : images) {
            // Check if image is already an image on the system of the same tag.
            if (image.getRepoTags().length == 1 && image.getRepoTags()[0].equals(dockerImage)) {
                // We want to try to kill the image and remove it.
                try {
                    dockerClient.killContainerCmd(image.getId()).exec();
                } catch (Exception ignored) {
                }
                try {
                    dockerClient.removeImageCmd(image.getId()).withForce(true).exec();
                } catch (Exception ignored) {
                }
                break;
            }
        }

        final String dockerImageName = "test-graph";
        // If there is a container of the same name, remove it.
        try {
            dockerClient.removeContainerCmd(dockerImageName).withForce(true).exec();
        } catch (Exception ignored) {
        }

        // Create port bindings and expose port 8182.
        final ExposedPort tcp8182 = ExposedPort.tcp(DEFAULT_PORT);
        final Ports portBindings = new Ports();
        portBindings.bind(tcp8182, Ports.Binding.bindPort(DEFAULT_PORT));

        // Create container with name, port, and environment set.
        final String containerId = dockerClient.createContainerCmd(dockerImage)
                .withName(dockerImageName)
                .withExposedPorts(tcp8182)
                .withHostConfig(new HostConfig().withPortBindings(portBindings))
                .withEnv(environmentVariables)
                .exec().getId();

        // Start the container.
        dockerClient.startContainerCmd(containerId).exec();
        dockerImageTagToContainerId.put(containerId, new DockerInfo(dockerImageName, 8182));

        // Wait 20 seconds for the container to have logs ready.
        try {
            Thread.sleep(waitTimeSeconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Need to check if the container is running.
        InspectContainerResponse.ContainerState containerState = dockerClient.inspectContainerCmd(containerId).exec().getState();

        if (Boolean.FALSE.equals(containerState.getRunning()) && !expectException) {
            throw new RuntimeException("Error failed to start container " + dockerImage +
                    " under image name '" + dockerImageName + "'. Container state: " + containerState.getStatus());
        }

        return containerId;
    }

    public Queue<String> getLogs(final String containerId) throws InterruptedException {
        final Queue<String> log = new ConcurrentLinkedQueue<>();
        final ResultCallback.Adapter<Frame> callback = dockerClient.logContainerCmd(containerId).withStdOut(true).withStdErr(true).exec(new ResultCallback.Adapter<Frame>() {
            @Override
            public void onNext(Frame frame) {
                log.add(new String(frame.getPayload()).trim());
            }
        });
        dockerClient.logContainerCmd(containerId).withStdOut(true).withStdErr(true).exec(callback).awaitCompletion();
        return log;
    }

    public boolean checkTmpFileExists(final String containerId) throws InterruptedException {
        // Create an exec command to test file existence
        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
                .withCmd("sh", "-c", "test -f /tmp/firefly-ready && echo FOUND_FILE || echo DID_NOT_FIND_FILE ")
                .withAttachStdout(true)
                .withAttachStderr(true)
                .exec();

        // Run the exec command and capture the output
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Exception[] eOutput = {null};
        dockerClient.execStartCmd(execCreateCmdResponse.getId())
                .exec(new ResultCallback.Adapter<>() {
                    @Override
                    public void onNext(com.github.dockerjava.api.model.Frame frame) {
                        try {
                            outputStream.write(frame.getPayload());
                        } catch (Exception e) {
                            eOutput[0] = e;
                        }
                    }
                }).awaitCompletion();

        if (eOutput[0] != null) {
            throw new RuntimeException(eOutput[0]);
        }

        // Check the output
        return outputStream.toString().trim().contains("FOUND_FILE");
    }

    public synchronized boolean versionExists(final String dockerImage, final String tag) {
        try {
            dockerClient.pullImageCmd(dockerImage).withTag(tag).start().awaitCompletion();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public synchronized void stopAllDockerImages() {
        for (final Container container : dockerClient.listContainersCmd().exec()) {
            try {
                for (int i = 0; i < container.getNames().length; i++) {
                    final String name = container.getNames()[i];
                    if (name != null && name.startsWith("/test-graph")) {
                        dockerClient.killContainerCmd(container.getId()).exec();
                        dockerClient.removeContainerCmd(container.getId()).withForce(true).exec();
                    }
                }
            } catch (final Exception e) {
                LOG.error("Failed to kill docker container: " + container.getId(), e);
            }
        }
    }
}
