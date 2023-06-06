package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class HealthcheckServer {
    private final Logger LOG = LoggerFactory.getLogger(HealthcheckServer.class);
    private final int port;
    public static final int DEFAULT_HEALTHCHECK_PORT = 9999;
    public static final String DEFAULT_HEALTHCHECK_PATH = "/healthcheck";
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final Vertx vertx = Vertx.vertx();
    private final AerospikeConnection ac;

    private HealthcheckServer(final Configuration config, final int port) {
        this.port = port;
        this.ac = new AerospikeConnection(config);
    }

    public static HealthcheckServer create(final Configuration config, final int port) {
        return new HealthcheckServer(config, port);
    }

    public void start() {
        // If this is started, do not start twice. This shouldn't happen.
        LOG.info("Starting HealthcheckServer on port {}.", port);
        if (started.getAndSet(true)) {
            LOG.warn("HealthcheckServer already started.");
            return;
        }

        // Create a router to handle requests.
        final Router router = Router.router(vertx);

        // Add a handler for the metrics endpoint - this picks up the default registry.
        router.get(DEFAULT_HEALTHCHECK_PATH).handler(new Handler<RoutingContext>() {
            @Override
            public void handle(final RoutingContext routingContext) {
                routingContext.response().end(String.valueOf(AerospikeConnection.InfoOps.isEnterprise(ac.getClient())));
            }
        });

        // Bootstrap http server with request handler on provided port.
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(port)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        LOG.info("HealthcheckServer is now listening on port {}.", port);
                    } else {
                        LOG.error("HealthcheckServer failed to bind with error {}.", res.cause().getMessage());
                    }
                });
    }
}
