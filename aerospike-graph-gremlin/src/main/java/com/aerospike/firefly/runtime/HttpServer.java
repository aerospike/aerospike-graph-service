package com.aerospike.firefly.runtime;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.runtime.metrics.FireflyMetricCollector;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class HttpServer {
    private final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
    private final int port;
    private final String prometheusPath;
    private final String healthcheckPath;
    public static final int DEFAULT_HTTP_PORT = 9090;
    public static final String DEFAULT_PROMETHEUS_PATH = "/metrics";
    public static final String DEFAULT_HEALTHCHECK_PATH = "/healthcheck";
    private static final int HEALTHCHECK_SUCCESS_CODE = 200;
    private static final int HEALTHCHECK_ERROR_CODE = 503;
    public static boolean PROMETHEUS_RENAME_ENABLED = true;
    private static AerospikeConnection db;
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final Vertx vertx = Vertx.vertx();

    private HttpServer(final int port, final String prometheusPath, final String healthcheckpath) {
        this.port = port;
        this.prometheusPath = prometheusPath;
        this.healthcheckPath = healthcheckpath;
    }

    public static HttpServer create(final int port, final String prometheusPath, final String healthcheckpath) {
        return new HttpServer(port, prometheusPath, healthcheckpath);
    }

    public static void registerGraphMetrics(final AerospikeConnection db) {
        PROMETHEUS_RENAME_ENABLED = db.PROMETHEUS_RENAME_ENABLED;
        try {
            CollectorRegistry.defaultRegistry.register(new FireflyMetricCollector(db));
        } catch (final IllegalArgumentException e) {
            // This happens if this is called multiple times because the collector is already registered, which is fine.
            // This will be the case in testing when graph is opened multiple times.
            if (!e.getMessage().contains("cluster_name_info is already in use by another Collector of type FireflyMetricCollector")) {
                throw e;
            }
        }
    }

    public static void registerHealthcheck(final AerospikeConnection db) {
        HttpServer.db = db;
    }

    // Not required except for bulk loader which hangs if it does not close this.
    public static void close() {
        if (vertx != null) {
            vertx.close();
        }
    }

    public void start() {
        // If this is started, do not start twice. This shouldn't happen.
        LOG.info("Starting HttpServer on port {}.", port);
        if (started.getAndSet(true)) {
            LOG.warn("HttpServer already started.");
            return;
        }

        // Register TinkerPop metrics with the default registry.
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(MetricManager.INSTANCE.getRegistry()));

        DefaultExports.initialize();

        // Create a router to handle requests.
        final Router router = Router.router(vertx);

        // Add a handler for the metrics endpoint - this picks up the default registry.
        router.get(prometheusPath).handler(new FireflyMetricRewiter());
        router.get(healthcheckPath).handler(routingContext -> {
            if (db != null && db.getClient().isConnected()) {
                routingContext.response().setStatusCode(HEALTHCHECK_SUCCESS_CODE).putHeader("content-type", "text/html").
                        end(String.valueOf(List.of(Map.of("status", "true"))));
            } else {
                routingContext.response().setStatusCode(HEALTHCHECK_ERROR_CODE).putHeader("content-type", "text/html").
                        end(String.valueOf(List.of(Map.of("status", "false"))));
            }
        });

        // Bootstrap http server with request handler on provided port.
        vertx.createHttpServer()
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

    private static class FireflyMetricRewiter implements Handler<RoutingContext> {

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

        /**
         * Construct a MetricsHandler for the default registry.
         */
        public FireflyMetricRewiter() {
            this(CollectorRegistry.defaultRegistry);
        }

        /**
         * Construct a MetricsHandler for the given registry.
         */
        public FireflyMetricRewiter(final CollectorRegistry registry) {
            this.registry = registry;
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

                    private List<String> listRename(final List<String> input) {
                        return input.stream().map(this::rename).collect(Collectors.toList());
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
                                                                    if (PROMETHEUS_RENAME_ENABLED) {
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
