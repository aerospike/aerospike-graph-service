package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoopBase;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestAerospikeClientConfigEventLoop {

    @Before
    public void before() {
        Assert.assertEquals(0, AerospikeConnection.DefaultAerospikeClientProvider.OPEN_COUNT.get());
    }

    @Test
    public void testLoopType() throws NoSuchFieldException, IllegalAccessException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Field field = NettyEventLoops.class.getDeclaredField("eventLoopType");
        field.setAccessible(true);

        config.setProperty("aerospike.client.eventLoop.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            Assert.assertTrue(client.getCluster().eventLoops instanceof NioEventLoops);
        }

        config.setProperty("aerospike.client.eventLoop.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            Assert.assertTrue(client.getCluster().eventLoops instanceof NettyEventLoops);
            final NettyEventLoops nettyEventLoops = (NettyEventLoops) client.getCluster().eventLoops;
            final EventLoopType loopType = (EventLoopType) field.get(nettyEventLoops);
            Assert.assertEquals("NETTY_NIO", loopType.name());
        }

        config.setProperty("aerospike.client.eventLoop.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            Assert.assertTrue(client.getCluster().eventLoops instanceof NettyEventLoops);
            final NettyEventLoops nettyEventLoops = (NettyEventLoops) client.getCluster().eventLoops;
            final EventLoopType loopType = (EventLoopType) field.get(nettyEventLoops);
            Assert.assertEquals("NETTY_EPOLL", loopType.name());
        }

        config.setProperty("aerospike.client.eventLoop.type", "invalid");
        try {
            AerospikeConnection.DefaultAerospikeClientProvider.connect(config);
            Assert.fail("Should have failed when trying to connect with an invalid event loop type.");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertEquals("Invalid event loop type provided: invalid", e.getMessage());
        }
    }

    @Test
    public void testLoopCount() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

        config.setProperty("aerospike.client.eventLoop.count", "3");
        config.setProperty("aerospike.client.eventLoop.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            Assert.assertEquals(3, client.getCluster().eventLoops.getSize());
        }

        config.setProperty("aerospike.client.eventLoop.count", "4");
        config.setProperty("aerospike.client.eventLoop.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            Assert.assertEquals(4, client.getCluster().eventLoops.getSize());
        }

        config.setProperty("aerospike.client.eventLoop.count", "5");
        config.setProperty("aerospike.client.eventLoop.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            Assert.assertEquals(5, client.getCluster().eventLoops.getSize());
        }

        config.setProperty("aerospike.client.eventLoop.count", "-1");
        try {
            AerospikeConnection.DefaultAerospikeClientProvider.connect(config);
            Assert.fail("Should have failed when trying to connect with an invalid event loop count.");
        } catch (final Exception e) {
            Assert.assertEquals("Value provided, \"-1\", for configuration key, \"aerospike.client.eventLoop.count\", is below the minimum acceptable value, \"0\".", e.getMessage());
        }
    }

    @Test
    public void testCommandsPerEventLoop() throws NoSuchFieldException, IllegalAccessException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Field field = EventLoopBase.class.getDeclaredField("maxCommandsInProcess");
        field.setAccessible(true);

        config.setProperty("aerospike.client.eventLoop.commands", "10");
        config.setProperty("aerospike.client.eventLoop.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(10, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventLoop.commands", "20");
        config.setProperty("aerospike.client.eventLoop.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(20, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.eventLoop.commands", "30");
        config.setProperty("aerospike.client.eventLoop.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
        final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
        Assert.assertEquals(30, field.get(eventLoop));
        }
    }

    @Test
    public void testDelayQueueSize() throws NoSuchFieldException, IllegalAccessException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final Field field = EventLoopBase.class.getDeclaredField("maxCommandsInQueue");
        field.setAccessible(true);

        config.setProperty("aerospike.client.delayQueue.size", "10");
        config.setProperty("aerospike.client.eventLoop.type", "DIRECT_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(10, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.delayQueue.size", "20");
        config.setProperty("aerospike.client.eventLoop.type", "NETTY_NIO");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(20, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.delayQueue.size", "30");
        config.setProperty("aerospike.client.eventLoop.type", "NETTY_EPOLL");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final AerospikeClient client = AerospikeConnection.DefaultAerospikeClientProvider.CLIENT;
            final EventLoopBase eventLoop = (EventLoopBase) client.getCluster().eventLoops.get(0);
            Assert.assertEquals(30, field.get(eventLoop));
        }

        config.setProperty("aerospike.client.delayQueue.size", "-1");
        try {
            AerospikeConnection.DefaultAerospikeClientProvider.connect(config);
            Assert.fail("Should have failed when trying to connect with an invalid max command queue count.");
        } catch (final Exception e) {
            Assert.assertEquals("Value provided, \"-1\", for configuration key, \"aerospike.client.delayQueue.size\", is below the minimum acceptable value, \"0\".", e.getMessage());
        }
    }
}
