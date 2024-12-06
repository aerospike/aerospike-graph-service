package com.aerospike.firefly.runtime.metrics;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.util.ReflectionHelper;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class ServerMetrics {
    private static final Logger logger = LoggerFactory.getLogger(ServerMetrics.class);
    private final String nettyQueueSizeMetricName = MetricRegistry.name("aerospike_graph_service_server_queue_size");
    private final String gremlinQueueSizeMetricName = MetricRegistry.name("aerospike_graph_service_gremlin_queue_size");
    private final GremlinServer gremlinServer;

    public ServerMetrics(final GremlinServer gremlinServer) {
        this.gremlinServer = gremlinServer;
    }

    public void start() {
        MetricManager.INSTANCE.getRegistry().register(nettyQueueSizeMetricName, (Gauge<Integer>) this::getNettyQueueSize);
        MetricManager.INSTANCE.getRegistry().register(gremlinQueueSizeMetricName, (Gauge<Integer>) this::getGremlinQueueSize);
    }

    public void shutDown() {
        logger.info("Shutting down server metrics.");
        MetricManager.INSTANCE.getRegistry().remove(nettyQueueSizeMetricName);
        MetricManager.INSTANCE.getRegistry().remove(gremlinQueueSizeMetricName);
    }

    private int getNettyQueueSize() {
        int pendingTasksCount = 0;

        final GremlinExecutor gremlinExecutor = gremlinServer.getServerGremlinExecutor().getGremlinExecutor();
        for (final EventExecutor eventExecutor : (EventLoopGroup) gremlinExecutor.getScheduledExecutorService()) {
            if (eventExecutor instanceof SingleThreadEventExecutor) {
                final SingleThreadEventExecutor singleExecutor = (SingleThreadEventExecutor) eventExecutor;
                pendingTasksCount += singleExecutor.pendingTasks();
            }
        }

        final EventLoopGroup bossGroup = (EventLoopGroup) ReflectionHelper.getFieldValue(gremlinServer, "bossGroup");
        for (final EventExecutor eventExecutor : bossGroup) {
            if (eventExecutor instanceof SingleThreadEventExecutor) {
                final SingleThreadEventExecutor singleExecutor = (SingleThreadEventExecutor) eventExecutor;
                pendingTasksCount += singleExecutor.pendingTasks();
            }
        }

        return pendingTasksCount;
    }

    private int getGremlinQueueSize() {
        final ExecutorService gremlinExecutorService = gremlinServer.getServerGremlinExecutor().getGremlinExecutorService();
        return ((ThreadPoolExecutor) gremlinExecutorService).getQueue().size();
    }
}
