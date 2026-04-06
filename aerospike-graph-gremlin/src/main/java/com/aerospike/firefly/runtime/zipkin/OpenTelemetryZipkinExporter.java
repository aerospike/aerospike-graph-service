package com.aerospike.firefly.runtime.zipkin;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.ServiceAttributes;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class OpenTelemetryZipkinExporter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryZipkinExporter.class);
    // Name of the service
    private static final String SERVICE_NAME = "ags-trace";
    private static Map<String, OpenTelemetryZipkinExporter> INSTANCES = new ConcurrentHashMap<>();

    private static final String metricPercentDuration = "percentDur";
    private static final String displayPercentDuration = "PercentDuration";
    private static final String metricTraverserCount = "traverserCount";
    private static final String displayTraverserCount = "UniqueElements";
    private static final String metricElementCount = "elementCount";
    private static final String displayElementCount = "TotalElements";

    private final Random querySampler = new Random();
    private final OpenTelemetry openTelemetry;
    private final SdkTracerProvider tracerProvider;
    private final String graphId;
    private final int minQueryThresholdMillis;
    private final int samplingPercentage;

    private OpenTelemetryZipkinExporter(final OpenTelemetry openTelemetry, final SdkTracerProvider tracerProvider,
                                        final String graphId, final int minQueryThresholdMillis,
                                        final int samplingPercentage) {
        this.openTelemetry = openTelemetry;
        this.tracerProvider = tracerProvider;
        this.graphId = graphId;
        this.minQueryThresholdMillis = minQueryThresholdMillis;
        this.samplingPercentage = samplingPercentage;
    }

    /**
     * Adds a SimpleSpanProcessor initialized with ZipkinSpanExporter to the TracerSdkProvider
     */
    static synchronized public OpenTelemetryZipkinExporter create(final String graphId,
                                                                  final String ip,
                                                                  final int port,
                                                                  final int minQueryThresholdMillis,
                                                                  final int samplingPercentage) {
        if (!INSTANCES.containsKey(graphId)) {
            healthCheck(ip, port);
            final String endpoint = String.format("http://%s:%s/api/v2/spans", ip, port);
            final ZipkinSpanExporter zipkinExporter = ZipkinSpanExporter.builder().setEndpoint(endpoint).build();

            final Resource serviceNameResource =
                    Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, SERVICE_NAME + " - " + graphId));

            // Set to process the spans by the Zipkin Exporter
            final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(zipkinExporter))
                    .setResource(Resource.getDefault().merge(serviceNameResource))
                    .build();
            final OpenTelemetrySdk openTelemetry =
                    OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

            final OpenTelemetryZipkinExporter exporter = new OpenTelemetryZipkinExporter(openTelemetry, tracerProvider,
                    graphId, minQueryThresholdMillis, samplingPercentage);
            INSTANCES.put(graphId, exporter);
        }

        // return the configured instance so it can be used for instrumentation.
        return INSTANCES.get(graphId);
    }

    public void exportQuery(final DefaultTraversalMetrics metrics, final String scopeName, final String traversal) {
        try {
            if (metrics.getDuration(TimeUnit.MILLISECONDS) >= this.minQueryThresholdMillis &&
                    querySampler.nextInt(100) < this.samplingPercentage) {
                final Tracer tracer = this.openTelemetry.getTracerProvider().get(scopeName);
                final SpanBuilder spanBuilder = tracer.spanBuilder(traversal);

                final AtomicReference<Instant> instant = new AtomicReference<>();
                instant.set(Instant.now().minusNanos(metrics.getDuration(TimeUnit.NANOSECONDS)));
                spanBuilder.setStartTimestamp(instant.get());
                final Span span = spanBuilder.startSpan();
                try (final Scope scope = span.makeCurrent()) {
                    writeSpan(metrics.getMetrics(), instant, tracer, span);
                } finally {
                    span.end(instant.get());
                }
            }
        } catch (final Exception e) {
            LOG.error("Unexpected failure occurred during Query Tracing export. Please check your Query Tracing endpoint.", e);
        }
    }

    private void writeSpan(final Collection<? extends Metrics> metrics, final AtomicReference<Instant> instant,
                           final Tracer tracer, final Span parent) {
        for (final Metrics m : metrics) {
            final SpanBuilder builder = tracer.spanBuilder(m.getName())
                    .setParent(parent.storeInContext(Context.current()));
            builder.setStartTimestamp(instant.get());
            final Span span = builder.startSpan();
            span.updateName(m.getName());
            for (final String key : m.getAnnotations().keySet()) {
                if (metricPercentDuration.equals(key)) {
                    span.setAttribute(displayPercentDuration, m.getAnnotations().get(key).toString());
                } else {
                    span.setAttribute(key, m.getAnnotations().get(key).toString());
                }
            }
            for (final String key : m.getCounts().keySet()) {
                if (metricTraverserCount.equals(key)) {
                    span.setAttribute(displayTraverserCount, m.getCounts().get(key).toString());
                } else if (metricElementCount.equals(key)) {
                    span.setAttribute(displayElementCount, m.getCounts().get(key).toString());
                } else {
                    span.setAttribute(key, m.getAnnotations().get(key).toString());
                }
            }
            try (final Scope scope = span.makeCurrent()) {
                final Collection<? extends Metrics> nested = m.getNested();
                if (!nested.isEmpty()) {
                    writeSpan(nested, new AtomicReference<>(instant.get()), tracer, span);
                }
            } finally {
                instant.set(instant.get().plusNanos(m.getDuration(TimeUnit.NANOSECONDS)));
                span.end(instant.get());
            }
        }
    }

    static private void healthCheck(final String ip, final int port) {
        LOG.info("Query Tracing is enabled.");
        LOG.info("Establishing connection to Query Tracing endpoint at IP {} and Port {}.", ip, port);
        final String endpoint = String.format("http://%s:%s/health", ip, port);
        int responseCode;
        try {
            final URL url = new URL(endpoint);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            responseCode = connection.getResponseCode();
        } catch (final Exception e) {
            throw new IllegalStateException("Connection to Query Tracing endpoint failed with no response. This is most likely due to an incorrect IP or Port. Error: " + e.getMessage(), e);
        }
        if (responseCode == 200) {
            LOG.info("Connection to Query Tracing endpoint at IP {} and Port {} successful.", ip, port);
        } else {
            throw new IllegalStateException("Connection to Query Tracing endpoint failed with response code: " + responseCode);
        }
    }

    @Override
    public synchronized void close() {
        if (INSTANCES.containsKey(this.graphId)) {
            this.tracerProvider.close();
            INSTANCES.remove(graphId);
        }
    }
}
