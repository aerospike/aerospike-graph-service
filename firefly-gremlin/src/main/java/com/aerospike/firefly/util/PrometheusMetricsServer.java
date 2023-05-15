package com.aerospike.firefly.util;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.hotspot.DefaultExports;
import io.prometheus.client.vertx.MetricsHandler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class PrometheusMetricsServer {
    private final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsServer.class);
    private final int port;
    private final String path;
    public static final int DEFAULT_PROMETHEUS_PORT = 9090;
    public static final String DEFAULT_PROMETHEUS_PATH = "/metrics";
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final Vertx vertx = Vertx.vertx();

    private PrometheusMetricsServer(final int port, final String path) {
        this.port = port;
        this.path = path;
    }

    public static PrometheusMetricsServer create(final int port, final String path) {
        return new PrometheusMetricsServer(port, path);
    }

    public void start () {
        // If this is started, do not start twice. This shouldn't happen.
        LOG.info("Starting PrometheusMetricsServer on port {}.", port);
        if (started.getAndSet(true)) {
            LOG.warn("PrometheusMetricsServer already started.");
            return;
        }

        // Register TinkerPop metrics with the default registry.
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(MetricManager.INSTANCE.getRegistry()));

        // Add default JVM metrics to the default registry.
        DefaultExports.initialize();

        // Create a router to handle requests.
        final Router router = Router.router(vertx);

        // Add a handler for the metrics endpoint - this picks up the default registry.
        router.get(DEFAULT_PROMETHEUS_PATH).handler(new MetricsHandler());
        router.get(DEFAULT_PROMETHEUS_PATH).failureHandler(new MetricsHandler());

        // Bootstrap http server with request handler on provided port.
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(port)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        LOG.info("PrometheusMetricsServer is now listening on port {}.", port);
                    } else {
                        LOG.error("PrometheusMetricsServer failed to bind with error {}.", res.cause().getMessage());
                    }
                });
    }
}
