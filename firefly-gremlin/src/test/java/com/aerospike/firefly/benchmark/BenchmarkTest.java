package com.aerospike.firefly.benchmark;


import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MINUTES)
public class BenchmarkTest {
    private static final GraphTraversalSourceFactory.DATASET DATASET_TYPE = GraphTraversalSourceFactory.DATASET.FLIGHTS;
    private static DriverRemoteConnection driverRemoteConnection = null;
    private static GraphTraversalSource g = null;

    @BeforeClass
    public static void setup() {
        driverRemoteConnection = DriverRemoteConnection.using("127.0.0.1", 8182);
        g = traversal().withRemote(driverRemoteConnection);
        GraphTraversalSourceFactory.loadGraph(g, DATASET_TYPE);
    }

    @AfterClass
    public static void shutdown() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception ignored) {
            }
        }
        if (driverRemoteConnection != null) {
            try {
                driverRemoteConnection.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void fireflyBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkTest.class.getSimpleName())
                .forks(1)
                .timeout(TimeValue.minutes(10)) // Timeout
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testBenchmark1(final Blackhole blackhole) throws Exception {
        List<Vertex> vertices = g.V().has("code", "AUS").out().out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }

    @Benchmark
    public void testBenchmark2(final Blackhole blackhole) throws Exception {
        List<Vertex> vertices = g.V().has("code", "AUS").out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }
}
