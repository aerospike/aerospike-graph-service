package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoopBase;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.config.FireflyConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestAerospikeClientConfigEventLoop {

    @Test
    public void testLoopType() throws Exception {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Field field = NettyEventLoops.class.getDeclaredField("eventLoopType");
        field.setAccessible(true);
        final Class provider = Class.forName("com.aerospike.firefly.io.aerospike.AerospikeConnection$DefaultAerospikeClientProvider");
        final Field clientField = provider.getDeclaredField("CLIENT");

        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            Assert.assertTrue(client.getCluster().eventLoops instanceof NioEventLoops);
        }

        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            Assert.assertTrue(client.getCluster().eventLoops instanceof NettyEventLoops);
            final NettyEventLoops nettyEventLoops = (NettyEventLoops) client.getCluster().eventLoops;
            final EventLoopType loopType = (EventLoopType) field.get(nettyEventLoops);
            Assert.assertEquals("NETTY_NIO", loopType.name());
        }

        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            Assert.assertTrue(client.getCluster().eventLoops instanceof NettyEventLoops);
            final NettyEventLoops nettyEventLoops = (NettyEventLoops) client.getCluster().eventLoops;
            final EventLoopType loopType = (EventLoopType) field.get(nettyEventLoops);
            Assert.assertEquals("NETTY_EPOLL", loopType.name());
        }

        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "invalid");
        try {
            final Method method = provider.getDeclaredMethod("connect", FireflyConfiguration.class);
            method.invoke(null, FireflyConfiguration.fromConfiguration(config));
            Assert.fail("Should have failed when trying to connect with an invalid event loop type.");
        } catch (final InvocationTargetException wrapper) {
            final Throwable e = wrapper.getCause();
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertEquals("Invalid event loop type provided: invalid", e.getMessage());
        }
    }

    @Test
    public void testLoopCount() throws Exception {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Class provider = Class.forName("com.aerospike.firefly.io.aerospike.AerospikeConnection$DefaultAerospikeClientProvider");
        final Field clientField = provider.getDeclaredField("CLIENT");

        config.setProperty("aerospike.client.clientPolicy.eventLoops.size", "3");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            Assert.assertEquals(3, client.getCluster().eventLoops.getSize());
        }

        config.setProperty("aerospike.client.clientPolicy.eventLoops.size", "4");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            Assert.assertEquals(4, client.getCluster().eventLoops.getSize());
        }

        config.setProperty("aerospike.client.clientPolicy.eventLoops.size", "5");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            Assert.assertEquals(5, client.getCluster().eventLoops.getSize());
        }

        config.setProperty("aerospike.client.clientPolicy.eventLoops.size", "-1");
        try {
            final Method method = provider.getDeclaredMethod("connect", FireflyConfiguration.class);
            method.invoke(null, FireflyConfiguration.fromConfiguration(config));
            Assert.fail("Should have failed when trying to connect with an invalid event loop count.");
        } catch (final Exception wrapper) {
            final Throwable e = wrapper.getCause();
            Assert.assertEquals("Value provided, \"-1\", for configuration key, \"aerospike.client.clientPolicy.eventLoops.size\", is below the minimum acceptable value, \"0\".", e.getMessage());
        }
    }

    @Test
    public void testCommandsPerEventLoop() throws Exception {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Field field = EventLoopBase.class.getDeclaredField("maxCommandsInProcess");
        field.setAccessible(true);
        final Class provider = Class.forName("com.aerospike.firefly.io.aerospike.AerospikeConnection$DefaultAerospikeClientProvider");
        final Field clientField = provider.getDeclaredField("CLIENT");

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInProcess", "10");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(10, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInProcess", "20");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(20, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInProcess", "30");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(30, field.get(eventLoop));
        }
    }

    @Test
    public void testDelayQueueSize() throws Exception {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Field field = EventLoopBase.class.getDeclaredField("maxCommandsInQueue");
        field.setAccessible(true);
        final Class provider = Class.forName("com.aerospike.firefly.io.aerospike.AerospikeConnection$DefaultAerospikeClientProvider");
        final Field clientField = provider.getDeclaredField("CLIENT");

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInQueue", "10");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(10, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInQueue", "20");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(20, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInQueue", "30");
        config.setProperty("aerospike.client.clientPolicy.eventLoops.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = (AerospikeClient) clientField.get(null);
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(30, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventPolicy.maxCommandsInQueue", "-1");
        try {
            final Method method = provider.getDeclaredMethod("connect", FireflyConfiguration.class);
            method.invoke(null, FireflyConfiguration.fromConfiguration(config));
            Assert.fail("Should have failed when trying to connect with an invalid max command queue count.");
        } catch (final Exception wrapper) {
            final Throwable e = wrapper.getCause();
            Assert.assertEquals("Value provided, \"-1\", for configuration key, \"aerospike.client.eventPolicy.maxCommandsInQueue\", is below the minimum acceptable value, \"0\".", e.getMessage());
        }
    }
}
