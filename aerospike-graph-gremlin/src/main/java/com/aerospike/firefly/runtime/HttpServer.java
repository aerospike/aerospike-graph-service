package com.aerospike.firefly.runtime;

import com.aerospike.firefly.runtime.metrics.FireflyMetricCollector;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class HttpServer {
    private final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
    private static final int DEFAULT_HTTP_PORT = 9090;
    private static final String DEFAULT_PROMETHEUS_PATH = "/metrics";
    private static final String DEFAULT_HEALTHCHECK_PATH = "/healthcheck";
    private static final int HEALTHCHECK_SUCCESS_CODE = 200;
    private static final int HEALTHCHECK_ERROR_CODE = 503;
    private final AtomicInteger started = new AtomicInteger();
    private final static Vertx vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true));
    private io.vertx.core.http.HttpServer vertxHttpServer;
    private Router router;
    private static FireflyMetricCollector fireflyMetricCollector;
    private static long serverStartTime;

    private static HttpServer INSTANCE;

    private HttpServer() {
    }

    public synchronized static HttpServer getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new HttpServer();
        }
        return INSTANCE;
    }

    private synchronized void init(final FireflyGraph graph) {
        final Configuration configuration = graph.configuration();
        final Object portConfig = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.HTTP_PORT, configuration);
        final int port = portConfig == null ? DEFAULT_HTTP_PORT : Integer.parseInt(portConfig.toString());
        final String prometheusPath = Optional.ofNullable(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.PROMETHEUS_PATH, configuration)).orElse(DEFAULT_PROMETHEUS_PATH);

        serverStartTime = System.currentTimeMillis();
        LOG.info("Starting HttpServer on port {}.", port);

        // register metrics only for first graph
        if (fireflyMetricCollector == null) {
            // should be only one FireflyMetricCollector
            fireflyMetricCollector = new FireflyMetricCollector(graph);
            CollectorRegistry.defaultRegistry.register(fireflyMetricCollector);
        }

        // Register TinkerPop metrics with the default registry.
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(MetricManager.INSTANCE.getRegistry()));

        DefaultExports.initialize();

        // Create a router to handle requests.
        router = Router.router(vertx);

        // Add a handler for the metrics endpoint - this picks up the default registry.
        router.get(prometheusPath).handler(new FireflyMetricRewriter(graph.getBaseGraph().getConfig().prometheusRenameEnabled));

        // Bootstrap http server with request handler on provided port.
        vertxHttpServer = vertx.createHttpServer();
        vertxHttpServer
                .requestHandler(router)
                .listen(port)
                .onComplete(res -> {
                    if (res.succeeded()) {
                        LOG.info("HttpServer is now listening on port {}.", port);
                    } else {
                        LOG.error("HttpServer failed to bind with error {}.", res.cause().getMessage());
                    }
                });
    }

    // Not required except for bulk loader which hangs if it does not close this.
    public void close() {
        // let's wait for other graphs
        if (started.decrementAndGet() != 0) {
            return;
        }

        if (vertxHttpServer != null) {
            vertxHttpServer.close();
            vertxHttpServer = null;
        }
    }

    public void start(final FireflyGraph graph) {
        if (started.incrementAndGet() == 1) {
            init(graph);
        }

        LOG.info("Configuring HttpServer for graph {}.", graph.getBaseGraph().getConfig().graphId);

        final Configuration configuration = graph.configuration();
        String healthcheckPath =
                Optional.ofNullable(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.HEALTHCHECK_PATH, configuration))
                        .orElse(DEFAULT_HEALTHCHECK_PATH);

        try {
            // wait for router
            while (router == null) Thread.sleep(1);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        final Handler<RoutingContext> handler = routingContext -> {
            final boolean isConnected = graph.getBaseGraph() != null && graph.getBaseGraph().getClusterIsConnected();
            final boolean isHealthy = !FireflyGraph.NEED_PREHEAT && isConnected;

            final long uptimeSeconds = (System.currentTimeMillis() - serverStartTime) / 1000;
            final String uptimeFormatted = DurationFormatUtils.formatDurationWords(
                    uptimeSeconds * 1000L,
                    true,
                    true
            );

            final JsonObject statusObject = new JsonObject()
                    .put("status", isHealthy ? "true" : "false")
                    .put("uptime", uptimeFormatted)
                    .put("version", FireflyGraph.FIREFLY_VERSION);
            final int statusCode = isHealthy ? HEALTHCHECK_SUCCESS_CODE : HEALTHCHECK_ERROR_CODE;

            routingContext.response()
                    .setStatusCode(statusCode)
                    .putHeader("content-type", "application/json")
                    .end(statusObject.encode());
        };

        router.get(healthcheckPath).handler(handler);
        router.get("/" + graph.getBaseGraph().getConfig().graphId + healthcheckPath).handler(handler);

        graph.getAdminServiceRegistry().appendHandlers(router);
    }

    private static class FireflyMetricRewriter implements Handler<RoutingContext> {

        /**
         * Wrap a Vert.x Buffer as a Writer so it can be used with
         * TextFormat writer
         */
        private static class BufferWriter extends Writer {

            private final Buffer buffer = Buffer.buffer();

            @Override
            public void write(final char[] cbuf, final int off, final int len) throws IOException {
                buffer.appendString(new String(cbuf, off, len));
            }

            @Override
            public void flush() {
                // NO-OP
            }

            @Override
            public void close() throws IOException {
                // NO-OP
            }

            public Buffer getBuffer() {
                return buffer;
            }
        }

        private final CollectorRegistry registry;
        private final Boolean prometheusRenameEnabled;

        /**
         * Construct a MetricsHandler for the default registry.
         */
        public FireflyMetricRewriter(final Boolean prometheusRenameEnabled) {
            this(CollectorRegistry.defaultRegistry, prometheusRenameEnabled);
        }

        /**
         * Construct a MetricsHandler for the given registry.
         */
        public FireflyMetricRewriter(final CollectorRegistry registry, final Boolean prometheusRenameEnabled) {
            this.registry = registry;
            this.prometheusRenameEnabled = prometheusRenameEnabled;
        }

        @Override
        public void handle(final RoutingContext ctx) {
            try {
                final String contentType = TextFormat.chooseContentType(ctx.request().headers().get("Accept"));
                final Enumeration<Collector.MetricFamilySamples> samples = registry.filteredMetricFamilySamples(parse(ctx.request()));

                final Enumeration<Collector.MetricFamilySamples> renamedSamples = new Enumeration<>() {
                    @Override
                    public boolean hasMoreElements() {
                        return samples.hasMoreElements();
                    }

                    private String rename(final String input) {
                        return "aerospike_graph_service_" +
                                input.replace("org_apache_tinkerpop_gremlin_server_", "");
                    }

                    @Override
                    public Collector.MetricFamilySamples nextElement() {
                        final Collector.MetricFamilySamples next = samples.nextElement();
                        return new Collector.MetricFamilySamples(
                                rename(next.name),
                                next.type,
                                next.help,
                                next.samples.stream().map(sample ->
                                                new Collector.MetricFamilySamples.Sample(
                                                        rename(sample.name),
                                                        sample.labelNames, // Names are things like 'metric' so don't want to rename.
                                                        sample.labelValues.stream().
                                                                map(v -> {
                                                                    if (prometheusRenameEnabled) {
                                                                        return v.
                                                                                replace(" ", "_").
                                                                                replace("-", "_").
                                                                                replace("'", "").
                                                                                replace("___", "_");
                                                                    } else {
                                                                        return v;
                                                                    }
                                                                }).
                                                                collect(Collectors.toList()),
                                                        sample.value)).
                                        collect(Collectors.toList()));
                    }
                };

                final BufferWriter writer = new BufferWriter();
                TextFormat.writeFormat(contentType, writer, renamedSamples);
                ctx.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", contentType)
                        .end(writer.getBuffer());
            } catch (final IOException e) {
                ctx.fail(e);
            }
        }

        private Set<String> parse(final HttpServerRequest request) {
            return new HashSet<>(request.params().getAll("name[]"));
        }
    }
}
