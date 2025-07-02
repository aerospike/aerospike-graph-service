package com.aerospike.firefly.performance;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ContainerPort;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.InvocationBuilder;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.apache.commons.configuration2.Configuration;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;


import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestAerospikeNetwork {
    private final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private FireflyGraph compressGraph;
    private FireflyGraph uncompressGraph;
    private DockerClient dockerClient;
    private int port;
    private String containerId;

    @Before
    public void setUp() throws Exception {
        dockerClient = initDockerClient();

        String host = config.getString("aerospike.client.host");
        if(host.contains(",")){
            host = host.split(",")[0];
        }
        if(host.contains(":")){
            port = Integer.parseInt(host.split(":")[1]);
        }else{
            port = config.getInt("aerospike.graph.port", 3000);
        }

        Optional<String> matchedContainer = findAerospikeServerContainerId();
        if (matchedContainer.isPresent()) {
            containerId = matchedContainer.get();
        } else {
            throw new IllegalStateException("No container found running Aerospike server");
        }
        config.setProperty("aerospike.client.compress", true);
        compressGraph = FireflyGraph.open(config);
        config.setProperty("aerospike.client.compress", false);
        uncompressGraph = FireflyGraph.open(config);

        compressGraph.getBaseGraph().dropDatabase(compressGraph, false);
    }

    @Test
    public void testCompressionFlag1KbChar() throws Exception {
        String oneKbString = String.valueOf('a').repeat(1024);
        uncompressGraph.traversal().addV("1KbA").property("goCrazy", oneKbString).next();

        long[] sumCompress = new long[] {0, 0, 0};
        long[] sumUncompress = new long[] {0, 0, 0};
        int loops = 4;
        for (int i = 0; i < loops; i++) {
            sumUncompress = addLongs(sumUncompress, getNetworkStats(uncompressGraph));
            sumCompress = addLongs(sumCompress, getNetworkStats(compressGraph));
        }

        assertStatsReduced(averageLong(sumUncompress, loops), averageLong(sumCompress, loops));
    }

    @Test
    public void testCompressionFlag1KEdge() throws Exception {
        Vertex v1 = uncompressGraph.traversal().addV("1KbA").next();
        for (int i = 0; i < 1000; i++) {
            uncompressGraph.traversal().addE("is" + i).to(v1).from(v1).next();
        }

        long[] sumCompress = new long[] {0, 0, 0};
        long[] sumUncompress = new long[] {0, 0, 0};
        int loops = 4;
        for (int i = 0; i < loops; i++) {
            sumUncompress = addLongs(sumUncompress, getNetworkStats(uncompressGraph));
            sumCompress = addLongs(sumCompress, getNetworkStats(compressGraph));
        }

        assertStatsReduced(averageLong(sumUncompress, loops), averageLong(sumCompress, loops));
    }

    private long[] addLongs(long[] a, long[] b) {
        return new long[] {a[0] + b[0], a[1] + b[1], a[2] + b[2]};
    }

    private long[] averageLong(long[] a, int num) {
        return new long[] {a[0] / num, a[1] / num, a[2] / num};
    }

    private StatisticNetworksConfig captureStats() {
        InvocationBuilder.AsyncResultCallback<Statistics> callback = new InvocationBuilder.AsyncResultCallback<>() {
            @Override
            public void onNext(Statistics stats) {
                super.onNext(stats);
                try {
                    this.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        dockerClient.statsCmd(containerId).exec(callback);
        return callback.awaitResult().getNetworks().values().iterator().next();
    }

    public long[] getNetworkStats(FireflyGraph graph) throws InterruptedException {
        StatisticNetworksConfig before = captureStats();
        graph.traversal().V().toList();
        Thread.sleep(2000);
        StatisticNetworksConfig after = captureStats();
        return getNetworkDelta(before, after);
    }

    public long[] getNetworkDelta(StatisticNetworksConfig beforeStat, StatisticNetworksConfig afterStat) {
        long[] after = new long[]{afterStat.getRxBytes(), afterStat.getTxBytes(), afterStat.getTxPackets()};
        long[] before = new long[]{beforeStat.getRxBytes(), beforeStat.getTxBytes(), beforeStat.getTxPackets()};
        return new long[]{after[0] - before[0], after[1] - before[1], after[2] - before[2]};
    }

    private void assertStatsReduced(long[] uncompressed, long[] compressed) {
        System.out.printf("Uncompressed: Rx=%d, Tx=%d, Pkts=%d%n", uncompressed[0], uncompressed[1], uncompressed[2]);
        System.out.printf("Compressed: Rx=%d, Tx=%d, Pkts=%d%n", compressed[0], compressed[1], compressed[2]);

        Assert.assertTrue(uncompressed[0] > compressed[0]);
        Assert.assertTrue(uncompressed[1] > compressed[1]);
        Assert.assertTrue(uncompressed[2] >= compressed[2]);
    }

    public Optional<String> findAerospikeServerContainerId() {
        List<Container> containers = dockerClient.listContainersCmd().exec();
        for (Container container : containers) {
            if (container.getImage().contains("aerospike-server") || container.getImage().contains("aerospike:ee")) {
                for(ContainerPort curPort : container.getPorts()){
                    Integer publicPort = curPort.getPublicPort();
                    Integer privatePort = curPort.getPrivatePort();
                    if ((publicPort != null && publicPort == port) ||
                            (privatePort != null && privatePort == port)) {
                        return Optional.of(container.getId());
                    }
                }
            }
        }
        return Optional.empty();
    }

    private DockerClient initDockerClient() {
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(30))
                .build();
        return DockerClientImpl.getInstance(config, httpClient);
    }

    @After
    public void tearDown() throws Exception {
        if (dockerClient != null) {
            dockerClient.close();
        }
        if (compressGraph != null) {
            compressGraph.close();
        }
        if (uncompressGraph != null) {
            uncompressGraph.close();
        }
    }
}