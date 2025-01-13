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
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;

import java.io.Closeable;
import java.time.Instant;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class OpenTelemetryZipkinExporter implements Closeable {
    // Name of the service
    private static final String SERVICE_NAME = "ags-trace";
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final Random QUERY_SAMPLER = new Random();
    private static OpenTelemetryZipkinExporter INSTANCE;

    private static final String metricPercentDuration = "percentDur";
    private static final String displayPercentDuration = "PercentDuration";
    private static final String metricTraverserCount = "traverserCount";
    private static final String displayTraverserCount = "UniqueElements";
    private static final String metricElementCount = "elementCount";
    private static final String displayElementCount = "TotalElements";

    private final OpenTelemetry openTelemetry;
    private final SdkTracerProvider tracerProvider;
    private final int minQueryThresholdMillis;
    private final int samplingPercentage;

    private OpenTelemetryZipkinExporter(final OpenTelemetry openTelemetry, final SdkTracerProvider tracerProvider,
                                        final int minQueryThresholdMillis, final int samplingPercentage) {
        this.openTelemetry = openTelemetry;
        this.tracerProvider = tracerProvider;
        this.minQueryThresholdMillis = minQueryThresholdMillis;
        this.samplingPercentage = samplingPercentage;
    }

    /**
     * Adds a SimpleSpanProcessor initialized with ZipkinSpanExporter to the TracerSdkProvider
     */
    static synchronized public OpenTelemetryZipkinExporter create(final String ip,
                                                                  final int port,
                                                                  final int minQueryThresholdMillis,
                                                                  final int samplingPercentage) {
        if (!INITIALIZED.getAndSet(true)) {
            final String endpoint = String.format("http://%s:%s/api/v2/spans", ip, port);
            final ZipkinSpanExporter zipkinExporter = ZipkinSpanExporter.builder().setEndpoint(endpoint).build();

            final Resource serviceNameResource =
                    Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, SERVICE_NAME));

            // Set to process the spans by the Zipkin Exporter
            final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(zipkinExporter))
                    .setResource(Resource.getDefault().merge(serviceNameResource))
                    .build();
            final OpenTelemetrySdk openTelemetry =
                    OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();

            // add a shutdown hook to shut down the SDK
            Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));
            INSTANCE = new OpenTelemetryZipkinExporter(openTelemetry, tracerProvider, minQueryThresholdMillis,
                    samplingPercentage);
        }

        // return the configured instance so it can be used for instrumentation.
        return INSTANCE;
    }

    public void exportQuery(final DefaultTraversalMetrics metrics, final String scopeName, final String traversal) {
        if (metrics.getDuration(TimeUnit.MILLISECONDS) >= this.minQueryThresholdMillis &&
                QUERY_SAMPLER.nextInt(100) < this.samplingPercentage) {
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

    @Override
    public synchronized void close() {
        if (INITIALIZED.getAndSet(false)) {
            this.tracerProvider.close();
        }
    }
}
