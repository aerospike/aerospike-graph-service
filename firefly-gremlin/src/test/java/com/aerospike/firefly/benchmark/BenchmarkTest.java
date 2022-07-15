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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
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
    private DriverRemoteConnection driverRemoteConnection = null;
    private GraphTraversalSource g = null;

    @BeforeClass
    public static void load() {
        System.out.println("Creating the DriverRemoteConnection.");
        DriverRemoteConnection driverRemoteConnection = DriverRemoteConnection.using("127.0.0.1", 8182);

        System.out.println("Creating the GraphTraversalSource.");
        GraphTraversalSource g = traversal().withRemote(driverRemoteConnection);

        System.out.println("Loading the graph.");
        GraphTraversalSourceFactory.loadGraph(g, DATASET_TYPE);

        try {
            g.close();
        } catch (Exception ignored) {
        }
        try {
            driverRemoteConnection.close();
        } catch (Exception ignored) {
        }
    }

    @Setup
    public void setup(BenchmarkParams benchmarkParams) {
        System.out.println("Creating the DriverRemoteConnection (setup).");
        driverRemoteConnection = DriverRemoteConnection.using("127.0.0.1", 8182);

        System.out.println("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(driverRemoteConnection);
    }

    @AfterClass
    public static void shutdown() {
        System.out.println("Shutting down.");
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
        System.out.println("testBenchmark1 g: " + g);
        List<Vertex> vertices = g.V().has("code", "AUS").out().out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }

    @Benchmark
    public void testBenchmark2(final Blackhole blackhole) throws Exception {
        System.out.println("testBenchmark2 g: " + g);
        List<Vertex> vertices = g.V().has("code", "AUS").out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }
}
